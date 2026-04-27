package org.wowtools.hppt.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 长度前缀帧的读写工具，用于hppt/rhppt协议
 * 帧格式: [Length:N字节][Payload]
 *
 * @author liuyu
 */
public class FrameIo {

    /**
     * 写帧: [N字节长度][payload]
     */
    public static void writeFrame(OutputStream out, byte[] payload, int lengthFieldLength) throws IOException {
        byte[] header = new byte[lengthFieldLength];
        int length = payload.length;
        for (int i = lengthFieldLength - 1; i >= 0; i--) {
            header[i] = (byte) (length & 0xFF);
            length >>= 8;
        }
        out.write(header);
        out.write(payload);
        out.flush();
    }

    /**
     * 读帧: 读取N字节长度头，再读取对应长度的payload
     *
     * @return payload字节数组，若连接关闭返回null
     */
    public static byte[] readFrame(InputStream in, int lengthFieldLength) throws IOException {
        byte[] lenBytes = new byte[lengthFieldLength];
        if (!readFully(in, lenBytes)) {
            return null;
        }
        int length = 0;
        for (int i = 0; i < lengthFieldLength; i++) {
            length = (length << 8) | (lenBytes[i] & 0xFF);
        }
        if (length == 0) {
            return new byte[0];
        }
        byte[] payload = new byte[length];
        if (!readFully(in, payload)) {
            return null;
        }
        return payload;
    }

    /**
     * 完整读取指定长度的字节
     *
     * @return true成功读取, false连接已关闭
     */
    public static boolean readFully(InputStream in, byte[] buf) throws IOException {
        int offset = 0;
        while (offset < buf.length) {
            int n = in.read(buf, offset, buf.length - offset);
            if (n == -1) {
                return false;
            }
            offset += n;
        }
        return true;
    }
}
