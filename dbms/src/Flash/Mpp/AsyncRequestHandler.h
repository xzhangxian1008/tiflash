// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/UnaryCallback.h>
#include <Common/grpcpp.h>
#include <Common/MPMCQueue.h>
#include <Flash/Mpp/ReceiverChannelWriter.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <grpcpp/alarm.h>
#include <grpcpp/completion_queue.h>

namespace DB
{
enum class AsyncRequestStage
{
    NEED_INIT,
    WAIT_MAKE_READER,
    WAIT_READ,
    WAIT_FINISH,
    WAIT_RETRY,
    FINISHED,
};

using Clock = std::chrono::system_clock;
using TimePoint = Clock::time_point;

constexpr Int32 max_retry_times = 10;
constexpr Int32 retry_interval_time = 1; // second

template <typename RPCContext, bool enable_fine_grained_shuffle>
class AsyncRequestHandler : public UnaryCallback<bool>
{
public:
    using Status = typename RPCContext::Status;
    using Request = typename RPCContext::Request;
    using AsyncReader = typename RPCContext::AsyncReader;

    AsyncRequestHandler(
        std::vector<MsgChannelPtr> * msg_channels,
        const std::shared_ptr<RPCContext> & context,
        const Request & req,
        const String & req_id,
        std::atomic<Int64> * data_size_in_queue,
        std::function<void(bool, const String &)> && notify_receiver_)
        : rpc_context(context)
        , cq(&(GRPCCompletionQueuePool::global_instance->pickQueue()))
        , request(&req)
        , req_info(fmt::format("tunnel{}+{}", req.send_task_id, req.recv_task_id))
        , log(Logger::get(req_id, req_info))
        , channel_writer(msg_channels, req_info, log, data_size_in_queue, ExchangeMode::Async)
        , is_receiver_notified(false)
        , notify_receiver(std::move(notify_receiver_))
    {
        packet = std::make_shared<TrackedMppDataPacket>();
        start();
    }

    // execute will be called by RPC framework so it should be as light as possible.
    void execute(bool & ok) override
    {
        switch (stage)
        {
        case AsyncRequestStage::WAIT_RETRY:
            start();
            break;
        case AsyncRequestStage::WAIT_MAKE_READER:
        {
            // Use lock to ensure reader is created already in reactor thread
            std::unique_lock lock(mu);
            if (!ok)
            {
                reader.reset();
                LOG_WARNING(log, "MakeReader fail. retry time: {}", retry_times);
                retryOrDone("Exchange receiver meet error : send async stream request fail");
            }
            else
            {
                stage = AsyncRequestStage::WAIT_READ;
                receivePacket();
                sendPacket();
            }
            break;
        }
        case AsyncRequestStage::WAIT_READ:
            if (ok)
            {
                receivePacket();
                sendPacket();
            }
            else
            {
                stage = AsyncRequestStage::WAIT_FINISH;
                reader->finish(finish_status, thisAsUnaryCallback());
            }
            break;
        case AsyncRequestStage::WAIT_FINISH:
            if (finish_status.ok())
                connectionDone("");
            else
            {
                LOG_WARNING(
                    log,
                    "Finish fail. err code: {}, err msg: {}, retry time {}",
                    finish_status.error_code(),
                    finish_status.error_message(),
                    retry_times);
                retryOrDone(fmt::format("Exchange receiver meet error : {}", finish_status.error_message()));
            }
            break;
        case AsyncRequestStage::FINISHED:
            connectionDone("Exchange receiver meet error : push packets fail");
            break;
        default:
            __builtin_unreachable();
        }
    }

    bool finished() const
    {
        return stage == AsyncRequestStage::FINISHED;
    }

    bool meetError() const { return meet_error; }
    const String & getErrMsg() const { return err_msg; }
    const LoggerPtr & getLog() const { return log; }

private:
    void receivePacket()
    {
        has_data = true;
        reader->read(packet, thisAsUnaryCallback());

        auto err_msg = getErrorFromPackets();
        if (!(err_msg.empty()))
            connectionDone(fmt::format("Exchange receiver meet error : {}", err_msg));
    }

    String getErrorFromPackets()
    {
        // step 1: check if there is error packet
        // only the last packet may has error, see execute().
        if (packet->hasError())
            return packet->error();

        // step 2: check memory overflow error
        packet->recomputeTrackedMem();
        if (packet->hasError())
            return packet->error();
        return "";
    }

    bool retriable() const
    {
        return !has_data && retry_times + 1 < max_retry_times;
    }

    void connectionDone(String && msg)
    {
        if (!msg.empty())
        {
            meet_error = true;
            err_msg = std::move(msg);
        }

        if (!is_receiver_notified)
        {
            is_receiver_notified = true;
            notify_receiver(meet_error, err_msg);
        }
        stage = AsyncRequestStage::FINISHED;
    }

    void start()
    {
        stage = AsyncRequestStage::WAIT_MAKE_READER;

        // Use lock to ensure async reader is unreachable from grpc thread before this function returns
        std::unique_lock lock(mu);
        rpc_context->makeAsyncReader(*request, reader, cq, thisAsUnaryCallback());
    }

    bool retryOrDone(String done_msg)
    {
        if (retriable())
        {
            ++retry_times;
            stage = AsyncRequestStage::WAIT_RETRY;

            // Let alarm put me into CompletionQueue after a while
            // , so that we can try to connect again.
            alarm.Set(cq, Clock::now() + std::chrono::seconds(retry_interval_time), this);
            return true;
        }
        else
        {
            connectionDone(std::move(done_msg));
            return false;
        }
    }

    void sendPacket()
    {
        if (!channel_writer.write<enable_fine_grained_shuffle>(request->source_index, packet))
        {
            // 
            stage = AsyncRequestStage::FINISHED;
        }

        // can't reuse packet since it is sent to readers.
        packet = std::make_shared<TrackedMppDataPacket>();
    }

    // in case of potential multiple inheritances.
    UnaryCallback<bool> * thisAsUnaryCallback()
    {
        return static_cast<UnaryCallback<bool> *>(this);
    }

    std::shared_ptr<RPCContext> rpc_context;
    grpc::Alarm alarm{};
    grpc::CompletionQueue * cq; // won't be null and do not delete this pointer
    const Request * request; // won't be null
    String req_info;
    LoggerPtr log;
    ReceiverChannelWriter channel_writer;

    bool meet_error = false;
    bool has_data = false;
    String err_msg;
    int retry_times = 0;
    AsyncRequestStage stage = AsyncRequestStage::NEED_INIT;

    std::shared_ptr<AsyncReader> reader;
    TrackedMppDataPacketPtr packet;
    Status finish_status = RPCContext::getStatusOK();
    std::mutex mu;

    bool is_receiver_notified;
    std::function<void(bool, const String &)> notify_receiver;
};
} // namespace DB
