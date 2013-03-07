package com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.io.Closeable;

/**
 * Wraps a Cassandra.Client object along with its Thrift protocol and transport
 * objects.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CTConnection implements Closeable {
    private final TTransport transport;
    private final Cassandra.Client client;

    public CTConnection(TTransport transport, Client client) {
        super();
        this.transport = transport;
        this.client = client;
    }

    public TTransport getTransport() {
        return transport;
    }

    public Cassandra.Client getClient() {
        return client;
    }

    @Override
    public void close() {
        if (transport != null && transport.isOpen())
            transport.close();
    }

    @Override
    public String toString() {
        return "CTConnection [transport=" + transport + ", client=" + client + "]";
    }

}