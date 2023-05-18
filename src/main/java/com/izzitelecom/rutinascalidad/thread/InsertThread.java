package com.izzitelecom.rutinascalidad.thread;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertThread extends Thread {

    PreparedStatement preparedStatement;
    Connection connection;
    int idThread;

    int total = 0;

    public InsertThread(PreparedStatement prepared, Connection conn, int i) {
        preparedStatement=prepared;
        connection =conn;
        idThread=i;
    }

    @Override
    public void run() {
        try {
            System.out.println(" iniciando hilo "+idThread);
            int[] inserted = preparedStatement.executeBatch();
            connection.close();
            total = inserted.length;
            System.out.println(" finalizando hilo "+idThread+" Insertando "+inserted.length+" registros");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public int getTotal(){
        return total;
    }
}
