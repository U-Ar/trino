/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.net.HostAndPort;
import io.airlift.units.DataSize;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.security.sasl.SaslClient;

import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static com.google.common.reflect.Reflection.newProxy;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTransport
{
    @ParameterizedTest
    @MethodSource("frameSizes")
    public void testSaslFrameSize(DataSize maxFrameSize, int frameSize, boolean accepted)
            throws Exception
    {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            server.setSoTimeout(10_000);
            Future<?> response = executor.submit(() -> {
                try (Socket socket = server.accept();
                        DataOutputStream output = new DataOutputStream(socket.getOutputStream())) {
                    output.writeInt(frameSize);
                    if (accepted) {
                        output.write(new byte[frameSize]);
                    }
                }
                return null;
            });

            SaslClient saslClient = newProxy(SaslClient.class, (_, method, _) -> switch (method.getName()) {
                case "getMechanismName" -> "TEST";
                case "isComplete" -> true;
                case "dispose" -> null;
                default -> throw new UnsupportedOperationException(method.toString());
            });
            HiveMetastoreAuthentication authentication = (rawTransport, _, _) -> {
                try {
                    // Bypass the handshake to exercise the real post-authentication SASL reader.
                    return new TSaslClientTransport(saslClient, rawTransport);
                }
                catch (TTransportException e) {
                    throw new RuntimeException(e);
                }
            };

            try (TTransport transport = Transport.create(
                    HostAndPort.fromParts(server.getInetAddress().getHostAddress(), server.getLocalPort()),
                    Optional.empty(),
                    Optional.empty(),
                    10_000,
                    10_000,
                    toIntExact(maxFrameSize.toBytes()),
                    authentication,
                    Optional.empty())) {
                assertThat(transport.getConfiguration().getMaxMessageSize()).isEqualTo(TConfiguration.DEFAULT_MAX_MESSAGE_SIZE);
                byte[] buffer = new byte[1];
                if (accepted) {
                    assertThat(transport.read(buffer, 0, buffer.length)).isEqualTo(1);
                }
                else {
                    assertThatThrownBy(() -> transport.read(buffer, 0, buffer.length))
                            .isInstanceOf(TTransportException.class)
                            .hasMessageContaining("Frame size (" + frameSize + ") larger than max length (" + maxFrameSize.toBytes() + ")!");
                }
            }
            response.get(10, SECONDS);
        }
    }

    static Stream<Arguments> frameSizes()
    {
        return Stream.of(
                Arguments.of(new ThriftMetastoreConfig().getMaxFrameSize(), TConfiguration.DEFAULT_MAX_FRAME_SIZE + 1, false),
                Arguments.of(DataSize.of(32, MEGABYTE), TConfiguration.DEFAULT_MAX_FRAME_SIZE + 1, true),
                Arguments.of(DataSize.of(1, KILOBYTE), 1024, true),
                Arguments.of(DataSize.of(1, KILOBYTE), 1025, false));
    }
}
