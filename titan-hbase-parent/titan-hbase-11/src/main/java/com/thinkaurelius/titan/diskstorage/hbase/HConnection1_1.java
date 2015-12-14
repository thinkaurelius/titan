package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HConnection1_1 implements ConnectionMask
{

    private final Connection cnx;

    public HConnection1_1(Connection cnx)
    {
        this.cnx = cnx;
    }

    @Override
    public TableMask getTable(String name) throws IOException
    {
        return new HTable1_1(cnx.getTable(TableName.valueOf(name)));
    }

    @Override
    public AdminMask getAdmin() throws IOException
    {
        return new HBaseAdmin1_1(new HBaseAdmin(cnx));
    }

    @Override
    public void close() throws IOException
    {
        cnx.close();
    }
}
