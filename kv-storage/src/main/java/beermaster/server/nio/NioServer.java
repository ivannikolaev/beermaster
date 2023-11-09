package beermaster.server.nio;

import beermaster.context.ApplicationContext;
import beermaster.server.Server;
import beermaster.server.ServerSettings;
import beermaster.server.handler.RoutingMessageHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

public class NioServer implements Server {
    private static final Logger log = Logger.getAnonymousLogger();
    private static final String CONNECTION_ACCEPT = "New connection from the client %s has been accepted";
    private static final int CLIENT_MESSAGE_CAPACITY = 1024;
    private final ServerSettings settings;
    private final ApplicationContext applicationContext;
    private final RoutingMessageHandler routingMessageHandler;

    public NioServer(ServerSettings settings, ApplicationContext applicationContext, RoutingMessageHandler routingMessageHandler) {
        this.settings = settings;
        this.applicationContext = applicationContext;
        this.routingMessageHandler = routingMessageHandler;
    }

    @Override
    public void start() throws IOException {
        try (Selector selector = Selector.open()) {
            try (ServerSocketChannel serverSocket = configureServerSocketChannel(selector)) {
                ByteBuffer buffer = ByteBuffer.allocate(CLIENT_MESSAGE_CAPACITY);
                while (applicationContext.isRunning()) {
                    selector.select();
                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> iter = selectedKeys.iterator();
                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();
                        buffer.clear();
                        handleSelectionKey(selector, serverSocket, buffer, key);
                        iter.remove();
                    }
                }
            }
            selector.keys()
                    .forEach(selectionKey -> {
                        try {
                            selectionKey.channel().close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    private void handleSelectionKey(Selector selector, ServerSocketChannel serverSocket, ByteBuffer buffer, SelectionKey key) throws IOException {
        if (key.isAcceptable()) {
            configureSocketChannel(selector, serverSocket);
        }
        if (key.isReadable()) {
            SocketChannel client = (SocketChannel) key.channel();
            int r = client.read(buffer);
            if (r == -1) {
                client.close();
            } else {
                routingMessageHandler.handle(client, buffer.array());
            }
        }
    }

    private void configureSocketChannel(Selector selector, ServerSocketChannel serverSocket) throws IOException {
        SocketChannel client = serverSocket.accept();
        log.info(CONNECTION_ACCEPT.formatted(client.getRemoteAddress().toString()));
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ);
    }

    private ServerSocketChannel configureServerSocketChannel(Selector selector) throws IOException {
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress(settings.port()));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        return serverSocket;
    }
}
