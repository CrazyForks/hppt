package org.wowtools.hppt.common.pojo;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnsafeByteOperations;
import lombok.Getter;
import org.wowtools.hppt.common.protobuf.ProtoMessage;
import org.wowtools.hppt.common.util.DebugConfig;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * session发/收的bytes，包含sessionId和具体bytes
 *
 * @author liuyu
 * @date 2023/11/17
 */
@Getter
public class SessionBytes {

    private static final AtomicInteger serialNumberBuilder;

    static {
        if (DebugConfig.OpenSerialNumber) {
            serialNumberBuilder = new AtomicInteger();
        } else {
            serialNumberBuilder = null;
        }
    }

    private final int sessionId;
    private final byte[] bytes;
    private final int serialNumber;

    public SessionBytes(int sessionId, byte[] bytes) {
        this.sessionId = sessionId;
        this.bytes = bytes;
        if (!DebugConfig.OpenSerialNumber) {
            serialNumber = 0;
        } else {
            serialNumber = serialNumberBuilder.incrementAndGet();
        }
    }

    public SessionBytes(byte[] pbBytes) {
        ProtoMessage.BytesPb pb;
        try {
            pb = ProtoMessage.BytesPb.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        sessionId = pb.getSessionId();
        bytes = pb.getBytes().toByteArray();
        if (!DebugConfig.OpenSerialNumber) {
            serialNumber = 0;
        } else {
            serialNumber = pb.getSerialNumber();
        }
    }

    protected SessionBytes(ProtoMessage.BytesPb pb) {
        sessionId = pb.getSessionId();
        bytes = pb.getBytes().toByteArray();
        if (!DebugConfig.OpenSerialNumber) {
            serialNumber = 0;
        } else {
            serialNumber = pb.getSerialNumber();
        }
    }

    public int getSerialNumber() {
        return serialNumber;
    }

    public ProtoMessage.BytesPb.Builder toProto() {
        ProtoMessage.BytesPb.Builder builder = ProtoMessage.BytesPb.newBuilder()
                .setBytes(UnsafeByteOperations.unsafeWrap(bytes))
                .setSessionId(sessionId);
        if (DebugConfig.OpenSerialNumber) {
            builder.setSerialNumber(serialNumber);
        }
        return builder;
    }


}
