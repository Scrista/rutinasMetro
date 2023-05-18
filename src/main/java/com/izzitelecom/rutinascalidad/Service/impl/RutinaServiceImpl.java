package com.izzitelecom.rutinascalidad.Service.impl;

import com.izzitelecom.rutinascalidad.Service.NotificacionesService;
import com.izzitelecom.rutinascalidad.Service.RutinaService;
import com.izzitelecom.rutinascalidad.thread.InsertThread;
import com.izzitelecom.rutinascalidad.tool.UtilDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
public class RutinaServiceImpl extends UtilDataSource implements RutinaService  {

    @Autowired
    NotificacionesService notificacionesService;
    private final String ADMINISTRADORES = "p-scristales@izzi.mx,lgaytan@izzi.mx,friosc@izzi.mx,mjimenezl@izzi.mx,vacruz@izzi.mx";
    @Override
    public void importaUtilizacionUPCALIDADMETRO() {



        String queryCalidad = "/*METRO*/\n" +
                "SELECT  DISTINCT\n" +
                "\t\t--Dias anteriores\n" +
                "          TO_CHAR(TRUNC(SYSDATE)-1,'YYYY-MM-DD') AS FECHA,\n" +
                "          ds.UP_ID,\n" +
                "          ds.UP_DESC,\n" +
                "          --ds.MAC_ID,\n" +
                "          tnHUB.TOPNAME AS HUB,\n" +
                "          tnCMTS.TOPNAME AS CMTS,\n" +
                "          tnMAC.TOPNAME AS DOMAIN,\n" +
                "          dc.cantidad_ds,\n" +
                "          dcm.count_mac cantidad_cms_max,\n" +
                "          tnMAC.TOPDESC AS US_MAC,\n" +
                "          COUNT(ds.UP_ID) AS MUESTRAS,\n" +
                "          ROUND(AVG(ds.MAX_UNIQUE_CMS),0) AS MAX_UNIQUE_CM,\n" +
                "          ROUND(AVG(ds.MAX_ACTIVE_CMS),0) AS MAX_ACTIVE_CM,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 95 THEN 1 ELSE 0 END) AS Mayor_a_95,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 90 AND ds.MAX_PERCENT_UTIL < 95 THEN 1 ELSE 0 END) AS Mayor_a_90,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 85 AND ds.MAX_PERCENT_UTIL < 90  THEN 1 ELSE 0 END) AS Mayor_a_85,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 80 AND ds.MAX_PERCENT_UTIL < 85  THEN 1 ELSE 0 END) AS Mayor_a_80,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 75 AND ds.MAX_PERCENT_UTIL < 80  THEN 1 ELSE 0 END) AS Mayor_a_75,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 70 AND ds.MAX_PERCENT_UTIL < 75  THEN 1 ELSE 0 END) AS Mayor_a_70,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 60 AND ds.MAX_PERCENT_UTIL < 70  THEN 1 ELSE 0 END) AS Mayor_a_60,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 50 AND ds.MAX_PERCENT_UTIL < 60  THEN 1 ELSE 0 END) AS Mayor_a_50,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 40 AND ds.MAX_PERCENT_UTIL < 50  THEN 1 ELSE 0 END) AS Mayor_a_40,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 30 AND ds.MAX_PERCENT_UTIL < 40  THEN 1 ELSE 0 END) AS Mayor_a_30,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 20 AND ds.MAX_PERCENT_UTIL < 30  THEN 1 ELSE 0 END) AS Mayor_a_20,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 10 AND ds.MAX_PERCENT_UTIL < 20  THEN 1 ELSE 0 END) AS Mayor_a_10,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL <= 10 THEN 1 ELSE 0 END) AS Menor_a_10,\n" +
                "          CASE\n" +
                "            WHEN COUNT(ds.UP_ID)/168 < 0.4 THEN 'Muestras Insuficientes'\n" +
                "          ELSE\n" +
                "            CASE\n" +
                "              WHEN ROUND(AVG(ds.MAX_UNIQUE_CMS),1) = 0 THEN 'Sin Equipos'\n" +
                "            ELSE\n" +
                "              CASE\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 95 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 95'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 90 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 90'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 85 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 85'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 80 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 80'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 75 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 75'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 70 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 70'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 60 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 60'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 50 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 50'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 40 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 40'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 30 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 30'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 20 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 20'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 10 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 10'\n" +
                "              ELSE\n" +
                "                'Menor a 10'\n" +
                "              END\n" +
                "            END\n" +
                "          END AS UTILIZACION,\n" +
                "          AVG(ds.UP_MINISLOT) AVG_UP_MINISLOT \n" +
                "        FROM STARGUS.UPSTREAM_HOUR_FACTS ds\n" +
                "        \n" +
                "        LEFT OUTER JOIN STARGUS.TOPOLOGY_NODE tnMAC\n" +
                "        ON ds.MAC_ID = tnMAC.TOPOLOGYID\n" +
                "        LEFT OUTER JOIN STARGUS.TOPOLOGY_NODE tnCMTS\n" +
                "        ON ds.CMTS_ID = tnCMTS.TOPOLOGYID\n" +
                "        LEFT OUTER JOIN STARGUS.TOPOLOGY_LINK tlHUB\n" +
                "        ON ds.CMTS_ID = tlHUB.TOPOLOGYID\n" +
                "        AND tlHUB.PARENTID_NODETYPEID = 1002\n" +
                "        LEFT OUTER JOIN STARGUS.TOPOLOGY_NODE tnHUB\n" +
                "        ON tnHUB.TOPOLOGYID = tlHUB.PARENTID\n" +
                "        LEFT OUTER JOIN STARGUS.TOPOLOGY_LINK tlMarket\n" +
                "        ON tnHUB.TOPOLOGYID = tlMarket.TOPOLOGYID\n" +
                "        AND tlMarket.PARENTID_NODETYPEID = 1001\n" +
                "        LEFT OUTER JOIN STARGUS.TOPOLOGY_NODE tnMarket\n" +
                "        ON tnMarket.TOPOLOGYID = tlMarket.PARENTID \n" +
                "        left outer join\n" +
                "        ( select\n" +
                "        count(distinct UP_ID) cantidad_ds,\n" +
                "        mac_id\n" +
                "        from \n" +
                "        STARGUS.UPSTREAM_HOUR_FACTS\n" +
                "        where \n" +
                "        --Dias anteriores\n" +
                "        TRUNC(HOUR_STAMP) >= TRUNC(SYSDATE)-7 AND TRUNC(HOUR_STAMP) < TRUNC(SYSDATE)\n" +
                "        group by\n" +
                "        mac_id) dc on dc.mac_id = ds.mac_id\n" +
                "        \n" +
                "        \n" +
                "        left outer join (\n" +
                "        select\n" +
                "        max(cantidad_mac) count_mac,\n" +
                "        mac_id \n" +
                "        from\n" +
                "              \n" +
                "        ( select distinct\n" +
                "        max(max_unique_cms) cantidad_mac,\n" +
                "        UP_ID,\n" +
                "        mac_id\n" +
                "        from \n" +
                "        STARGUS.UPSTREAM_HOUR_FACTS\n" +
                "        where \n" +
                "        --Dias anteriores\n" +
                "        TRUNC(HOUR_STAMP) >= TRUNC(SYSDATE)-7 AND TRUNC(HOUR_STAMP) < TRUNC(SYSDATE)\n" +
                "        group by\n" +
                "        UP_ID,mac_id) \n" +
                "        \n" +
                "        group by mac_id) dcm on dcm.mac_id = ds.mac_id\n" +
                "        \n" +
                "        \n" +
                "        WHERE \n" +
                "        --Dias anteriores\n" +
                "          TRUNC(ds.HOUR_STAMP) >= TRUNC(SYSDATE)-7 AND TRUNC(ds.HOUR_STAMP) < TRUNC(SYSDATE)\n" +
                "          \n" +
                "        GROUP BY\n" +
                "        --Dias anteriores\n" +
                "          TRUNC(SYSDATE)-1,\n" +
                "          ds.UP_ID,\n" +
                "          ds.UP_DESC,\n" +
                "        --  ds.MAC_ID,\n" +
                "          --  ds.CMTS_ID,\n" +
                "          -- tnMarket.TOPNAME,\n" +
                "          tnHUB.TOPNAME,\n" +
                "          tnCMTS.TOPNAME,\n" +
                "          tnMAC.TOPNAME,\n" +
                "          tnMAC.TOPDESC,\n" +
                "           dcm.count_mac,\n" +
                "          dc.cantidad_ds";

        String compiledQuery = "INSERT INTO infest.BD_SAA_UTILIZACION_UP " +
                "(  FECHA," +
                "    UP_ID  ," +
                "    UP_DESC ," +
                "    HUB  ," +
                "    CMTS  ," +
                "    DOMAIN  ," +
                "    CANTIDAD_UP  ," +
                "    CANTIDAD_CMS_MAX ," +
                "    US_MAC ," +
                "    MUESTRAS ," +
                "    MAX_UNIQUE_CM ," +
                "    MAX_ACTIVE_CM  ," +
                "    MAYOR_A_95   ," +
                "    MAYOR_A_90  ," +
                "    MAYOR_A_85 ," +
                "    MAYOR_A_80 ," +
                "    MAYOR_A_75  ," +
                "    MAYOR_A_70   ," +
                "    MAYOR_A_60  ," +
                "    MAYOR_A_50    ," +
                "    MAYOR_A_40   ," +
                "    MAYOR_A_30    ," +
                "    MAYOR_A_20   ," +
                "    MAYOR_A_10 ," +
                "    MENOR_A_10   ," +
                "    UTILIZACION    ," +
                "    AVG_UP_MINISLOT  ) " +
                "VALUES (?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?) ";


        try (Connection connection = getExternalMETROConnectionFromPool();) {


            System.out.println("INSERTANDO UTILIZACIONUP de METRO");
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryCalidad);


            Connection infest = getOracleConnectionFromPool();


//            rutinaDao.truncateOrdenesCompletadas();

            List<Connection> conexiones = new ArrayList<>();
            conexiones.add(infest);


            PreparedStatement preparedStatement = conexiones.get(0).prepareStatement(compiledQuery);


            int cantidad = 0;
            int hilo = 0;
            List<InsertThread> hilos = new ArrayList<>();

            List<PreparedStatement> lotes = new ArrayList<>();

            lotes.add(preparedStatement);


            while (resultSet.next()) {


                lotes.get(hilo).setDate(1, resultSet.getDate("FECHA"));
                lotes.get(hilo).setString(2, resultSet.getString("UP_ID"));
                lotes.get(hilo).setString(3, resultSet.getString("UP_DESC"));
                lotes.get(hilo).setString(4, resultSet.getString("HUB"));
                lotes.get(hilo).setString(5, resultSet.getString("CMTS"));
                lotes.get(hilo).setString(6, resultSet.getString("DOMAIN"));


                lotes.get(hilo).setInt(7, resultSet.getInt("CANTIDAD_DS"));
                lotes.get(hilo).setInt(8, resultSet.getInt("CANTIDAD_CMS_MAX"));
                lotes.get(hilo).setString(9, resultSet.getString("US_MAC"));

                lotes.get(hilo).setDouble(10, resultSet.getDouble("MUESTRAS"));
                lotes.get(hilo).setDouble(11, resultSet.getDouble("MAX_UNIQUE_CM"));
                lotes.get(hilo).setDouble(12, resultSet.getDouble("MAX_ACTIVE_CM"));
                lotes.get(hilo).setDouble(13, resultSet.getDouble("MAYOR_A_95"));
                lotes.get(hilo).setDouble(14, resultSet.getDouble("MAYOR_A_90"));
                lotes.get(hilo).setDouble(15, resultSet.getDouble("MAYOR_A_85"));
                lotes.get(hilo).setDouble(16, resultSet.getDouble("MAYOR_A_80"));
                lotes.get(hilo).setDouble(17, resultSet.getDouble("MAYOR_A_75"));
                lotes.get(hilo).setDouble(18, resultSet.getDouble("MAYOR_A_70"));
                lotes.get(hilo).setDouble(19, resultSet.getDouble("MAYOR_A_60"));
                lotes.get(hilo).setDouble(20, resultSet.getDouble("MAYOR_A_50"));
                lotes.get(hilo).setDouble(21, resultSet.getDouble("MAYOR_A_40"));
                lotes.get(hilo).setDouble(22, resultSet.getDouble("MAYOR_A_30"));
                lotes.get(hilo).setDouble(23, resultSet.getDouble("MAYOR_A_20"));
                lotes.get(hilo).setDouble(24, resultSet.getDouble("MAYOR_A_10"));
                lotes.get(hilo).setDouble(25, resultSet.getDouble("MENOR_A_10"));


                lotes.get(hilo).setString(26, resultSet.getString("UTILIZACION"));

                lotes.get(hilo).setDouble(27, resultSet.getDouble("AVG_UP_MINISLOT"));


                if (cantidad < 500000) {


                    lotes.get(hilo).addBatch();
                    cantidad++;
                } else {

                    lotes.get(hilo).addBatch();

                    InsertThread h1 = new InsertThread(lotes.get(hilo), conexiones.get(hilo), hilo);

                    conexiones.add(getOracleConnectionFromPool());

                    lotes.add(conexiones.get(hilo + 1).prepareStatement(compiledQuery));


                    hilos.add(h1);
                    hilos.get(hilo).start();

                    cantidad = 0;
                    hilo++;
                }


            }

            InsertThread h1 = new InsertThread(lotes.get(hilo), conexiones.get(hilo), hilo);
            hilos.add(h1);
            hilos.get(hilo).start();


            resultSet.close();
            statement.close();


        } catch (     SQLException e) {
            e.printStackTrace();

            notificacionesService.enviaNotificacionGrupalPorTeams(ADMINISTRADORES, "FALLA CARGA importaUtilizacionUPCALIDADMETRO", e.getMessage());

        }

    }

    @Override
    public void importaUTILIZACIONDSMETRO() {


        String queryCalidad = "SELECT  DISTINCT\n" +
                "\t\t--Dias anteriores\n" +
                "          TO_CHAR(TRUNC(SYSDATE)-1,'YYYY-MM-DD') AS FECHA,\n" +
                "          ds.DOWN_ID,\n" +
                "          ds.DOWN_DESC,\n" +
                "          --ds.MAC_ID,\n" +
                "          tnHUB.TOPNAME AS HUB,\n" +
                "          tnCMTS.TOPNAME AS CMTS,\n" +
                "          tnMAC.TOPNAME AS DOMAIN,\n" +
                "          dc.cantidad_ds,\n" +
                "          dcm.count_mac cantidad_cms_max,\n" +
                "          tnMAC.TOPDESC AS US_MAC,\n" +
                "          COUNT(ds.DOWN_ID) AS MUESTRAS,\n" +
                "          ROUND(AVG(ds.MAX_UNIQUE_CMS),0) AS MAX_UNIQUE_CM,\n" +
                "          ROUND(AVG(ds.MAX_ACTIVE_CMS),0) AS MAX_ACTIVE_CM,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 95 THEN 1 ELSE 0 END) AS Mayor_a_95,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 90 AND ds.MAX_PERCENT_UTIL < 95 THEN 1 ELSE 0 END) AS Mayor_a_90,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 85 AND ds.MAX_PERCENT_UTIL < 90  THEN 1 ELSE 0 END) AS Mayor_a_85,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 80 AND ds.MAX_PERCENT_UTIL < 85  THEN 1 ELSE 0 END) AS Mayor_a_80,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 75 AND ds.MAX_PERCENT_UTIL < 80  THEN 1 ELSE 0 END) AS Mayor_a_75,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 70 AND ds.MAX_PERCENT_UTIL < 75  THEN 1 ELSE 0 END) AS Mayor_a_70,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 60 AND ds.MAX_PERCENT_UTIL < 70  THEN 1 ELSE 0 END) AS Mayor_a_60,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 50 AND ds.MAX_PERCENT_UTIL < 60  THEN 1 ELSE 0 END) AS Mayor_a_50,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 40 AND ds.MAX_PERCENT_UTIL < 50  THEN 1 ELSE 0 END) AS Mayor_a_40,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 30 AND ds.MAX_PERCENT_UTIL < 40  THEN 1 ELSE 0 END) AS Mayor_a_30,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 20 AND ds.MAX_PERCENT_UTIL < 30  THEN 1 ELSE 0 END) AS Mayor_a_20,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 10 AND ds.MAX_PERCENT_UTIL < 20  THEN 1 ELSE 0 END) AS Mayor_a_10,\n" +
                "          SUM(CASE WHEN ds.MAX_PERCENT_UTIL <= 10 THEN 1 ELSE 0 END) AS Menor_a_10,\n" +
                "          CASE\n" +
                "            WHEN COUNT(ds.DOWN_ID)/168 < 0.4 THEN 'Muestras Insuficientes'\n" +
                "          ELSE\n" +
                "            CASE\n" +
                "              WHEN ROUND(AVG(ds.MAX_UNIQUE_CMS),1) = 0 THEN 'Sin Equipos'\n" +
                "            ELSE\n" +
                "              CASE\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 95 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 95'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 90 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 90'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 85 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 85'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 80 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 80'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 75 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 75'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 70 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 70'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 60 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 60'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 50 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 50'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 40 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 40'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 30 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 30'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 20 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 20'\n" +
                "                WHEN SUM(CASE WHEN ds.MAX_PERCENT_UTIL > 10 THEN 1 ELSE 0 END) > 11 THEN 'Mayor a 10'\n" +
                "              ELSE\n" +
                "                'Menor a 10'\n" +
                "              END\n" +
                "            END\n" +
                "          END AS UTILIZACION\n" +
                "        FROM STARGUS.DOWNSTREAM_HOUR_FACTS ds\n" +
                "        \n" +
                "        LEFT OUTER JOIN STARGUS.TOPOLOGY_NODE tnMAC\n" +
                "        ON ds.MAC_ID = tnMAC.TOPOLOGYID\n" +
                "        LEFT OUTER JOIN STARGUS.TOPOLOGY_NODE tnCMTS\n" +
                "        ON ds.CMTS_ID = tnCMTS.TOPOLOGYID\n" +
                "        LEFT OUTER JOIN STARGUS.TOPOLOGY_LINK tlHUB\n" +
                "        ON ds.CMTS_ID = tlHUB.TOPOLOGYID\n" +
                "        AND tlHUB.PARENTID_NODETYPEID = 1002\n" +
                "        LEFT OUTER JOIN STARGUS.TOPOLOGY_NODE tnHUB\n" +
                "        ON tnHUB.TOPOLOGYID = tlHUB.PARENTID\n" +
                "        LEFT OUTER JOIN STARGUS.TOPOLOGY_LINK tlMarket\n" +
                "        ON tnHUB.TOPOLOGYID = tlMarket.TOPOLOGYID\n" +
                "        AND tlMarket.PARENTID_NODETYPEID = 1001\n" +
                "        LEFT OUTER JOIN STARGUS.TOPOLOGY_NODE tnMarket\n" +
                "        ON tnMarket.TOPOLOGYID = tlMarket.PARENTID \n" +
                "        left outer join\n" +
                "        ( select\n" +
                "        count(distinct down_id) cantidad_ds,\n" +
                "        mac_id\n" +
                "        from \n" +
                "        STARGUS.DOWNSTREAM_HOUR_FACTS\n" +
                "        where \n" +
                "        --Dias anteriores\n" +
                "        TRUNC(HOUR_STAMP) >= TRUNC(SYSDATE)-7 AND TRUNC(HOUR_STAMP) < TRUNC(SYSDATE)\n" +
                "        group by\n" +
                "        mac_id) dc on dc.mac_id = ds.mac_id\n" +
                "        \n" +
                "        \n" +
                "        left outer join (\n" +
                "        select\n" +
                "        max(cantidad_mac) count_mac,\n" +
                "        mac_id \n" +
                "        from\n" +
                "              \n" +
                "        ( select distinct\n" +
                "        max(max_unique_cms) cantidad_mac,\n" +
                "        down_id,\n" +
                "        mac_id\n" +
                "        from \n" +
                "        STARGUS.DOWNSTREAM_HOUR_FACTS\n" +
                "        where \n" +
                "        --Dias anteriores\n" +
                "        TRUNC(HOUR_STAMP) >= TRUNC(SYSDATE)-7 AND TRUNC(HOUR_STAMP) < TRUNC(SYSDATE)\n" +
                "        group by\n" +
                "        down_id,mac_id) \n" +
                "        \n" +
                "        group by mac_id) dcm on dcm.mac_id = ds.mac_id\n" +
                "        \n" +
                "        \n" +
                "        WHERE \n" +
                "        --Dias anteriores\n" +
                "          TRUNC(ds.HOUR_STAMP) >= TRUNC(SYSDATE)-7 AND TRUNC(ds.HOUR_STAMP) < TRUNC(SYSDATE)\n" +
                "          \n" +
                "        GROUP BY\n" +
                "        --Dias anteriores\n" +
                "          TRUNC(SYSDATE)-1,\n" +
                "          ds.DOWN_ID,\n" +
                "          ds.DOWN_DESC,\n" +
                "        --  ds.MAC_ID,\n" +
                "          --  ds.CMTS_ID,\n" +
                "          -- tnMarket.TOPNAME,\n" +
                "          tnHUB.TOPNAME,\n" +
                "          tnCMTS.TOPNAME,\n" +
                "          tnMAC.TOPNAME,\n" +
                "          tnMAC.TOPDESC,\n" +
                "           dcm.count_mac,\n" +
                "          dc.cantidad_ds";

        String compiledQuery = "INSERT INTO infest.BD_SAA_UTILIZACION_DS " +
                "(   FECHA  ," +
                "    DS_ID  ," +
                "    DS_DESC     ," +
                "    HUB        ," +
                "    CMTS         ," +
                "    DOMAIN        ," +
                "    CANTIDAD_DS  ," +
                "    CANTIDAD_CMS_MAX ," +
                "    DS_MAC         ," +
                "    MUESTRAS       ," +
                "    MAX_UNIQUE_CM   ," +
                "    MAX_ACTIVE_CM   ," +
                "    MAYOR_A_95     ," +
                "    MAYOR_A_90      ," +
                "    MAYOR_A_85     ," +
                "    MAYOR_A_80 ," +
                "    MAYOR_A_75      ," +
                "    MAYOR_A_70  ," +
                "    MAYOR_A_60     ," +
                "    MAYOR_A_50     ," +
                "    MAYOR_A_40      ," +
                "    MAYOR_A_30     ," +
                "    MAYOR_A_20   ," +
                "    MAYOR_A_10      ," +
                "    MENOR_A_10      ," +
                "    UTILIZACION      ) " +
                "VALUES (?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?,?," +
                "?) ";

        try (Connection connection = getExternalMETROConnectionFromPool();) {


            System.out.println("INSERTANDO UTILIZACIONUP de METRO");
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryCalidad);


            Connection infest = getOracleConnectionFromPool();


//            rutinaDao.truncateOrdenesCompletadas();

            List<Connection> conexiones = new ArrayList<>();
            conexiones.add(infest);


            PreparedStatement preparedStatement = conexiones.get(0).prepareStatement(compiledQuery);


            int cantidad = 0;
            int hilo = 0;
            List<InsertThread> hilos = new ArrayList<>();

            List<PreparedStatement> lotes = new ArrayList<>();

            lotes.add(preparedStatement);


            while (resultSet.next()) {


                lotes.get(hilo).setDate(1, resultSet.getDate("FECHA"));
                lotes.get(hilo).setString(2, resultSet.getString("DOWN_ID"));
                lotes.get(hilo).setString(3, resultSet.getString("DOWN_DESC"));
                lotes.get(hilo).setString(4, resultSet.getString("HUB"));
                lotes.get(hilo).setString(5, resultSet.getString("CMTS"));
                lotes.get(hilo).setString(6, resultSet.getString("DOMAIN"));


                lotes.get(hilo).setInt(7, resultSet.getInt("CANTIDAD_DS"));
                lotes.get(hilo).setInt(8, resultSet.getInt("CANTIDAD_CMS_MAX"));
                lotes.get(hilo).setString(9, resultSet.getString("US_MAC"));

                lotes.get(hilo).setDouble(10, resultSet.getDouble("MUESTRAS"));

                lotes.get(hilo).setDouble(11, resultSet.getDouble("MAX_UNIQUE_CM"));
                lotes.get(hilo).setDouble(12, resultSet.getDouble("MAX_ACTIVE_CM"));
                lotes.get(hilo).setDouble(13, resultSet.getDouble("MAYOR_A_95"));
                lotes.get(hilo).setDouble(14, resultSet.getDouble("MAYOR_A_90"));
                lotes.get(hilo).setDouble(15, resultSet.getDouble("MAYOR_A_85"));
                lotes.get(hilo).setDouble(16, resultSet.getDouble("MAYOR_A_80"));
                lotes.get(hilo).setDouble(17, resultSet.getDouble("MAYOR_A_75"));
                lotes.get(hilo).setDouble(18, resultSet.getDouble("MAYOR_A_70"));
                lotes.get(hilo).setDouble(19, resultSet.getDouble("MAYOR_A_60"));
                lotes.get(hilo).setDouble(20, resultSet.getDouble("MAYOR_A_50"));
                lotes.get(hilo).setDouble(21, resultSet.getDouble("MAYOR_A_40"));
                lotes.get(hilo).setDouble(22, resultSet.getDouble("MAYOR_A_30"));
                lotes.get(hilo).setDouble(23, resultSet.getDouble("MAYOR_A_20"));
                lotes.get(hilo).setDouble(24, resultSet.getDouble("MAYOR_A_10"));
                lotes.get(hilo).setDouble(25, resultSet.getDouble("MENOR_A_10"));


                lotes.get(hilo).setString(26, resultSet.getString("UTILIZACION"));


                if (cantidad < 500000) {


                    lotes.get(hilo).addBatch();
                    cantidad++;
                } else {

                    lotes.get(hilo).addBatch();

                    InsertThread h1 = new InsertThread(lotes.get(hilo), conexiones.get(hilo), hilo);

                    conexiones.add(getOracleConnectionFromPool());

                    lotes.add(conexiones.get(hilo + 1).prepareStatement(compiledQuery));


                    hilos.add(h1);
                    hilos.get(hilo).start();

                    cantidad = 0;
                    hilo++;
                }


            }

            InsertThread h1 = new InsertThread(lotes.get(hilo), conexiones.get(hilo), hilo);
            hilos.add(h1);
            hilos.get(hilo).start();


            resultSet.close();
            statement.close();


        } catch (Exception e) {
            e.printStackTrace();

            notificacionesService.enviaNotificacionGrupalPorTeams(ADMINISTRADORES, "FALLA CARGA importaUtilizacionUPCALIDADMETRO", e.getMessage());

        }
    }

    @Override
    public void importaInterfacesDWMETRO() {
        try(Connection externa = getExternalMETROConnectionFromPool();
        ) {


            String compiledQuery = "INSERT INTO CAT_INTERFACES_DW (" +
                    "CMTS,INTERFACE_ID,INTERFACE_DESC,MAC_NODO,PARENTID,TIPO_INTERFAZ,ORIGEN) " +
                    "VALUES (?,?,?,?,?,?,?)";


            String queryExterna = " " +
                    "SELECT distinct tm.hostip                                       cmts, " +
                    "                tm.topologyid                                   down_id, " +
                    "                tm.topdesc, " +
                    "                tm.macaddr                                      mac_nodo, " +
                    "                mn.parentid, " +
                    "                case when tm.nodetypeid = 128 then 1 else 2 end tipo, " +
                    "                2                                               origen " +
                    " " +
                    "from Stargus.topology_node tm " +
                    "         left outer join stargus.topology_link mn " +
                    "                         on tm.topologyid = mn.topologyid " +
                    "                             and mn.parentid_nodetypeid = 2001 " +
                    " " +
                    "where tm.nodetypeid in (128, 129) ";


            Statement statement = externa.createStatement();

            ResultSet resultSet = statement.executeQuery(queryExterna);

            PreparedStatement preparedStatement;
            Connection connection = getOracleConnectionFromPool();



            List<Connection> conexiones = new ArrayList<>();
            conexiones.add(connection);

            preparedStatement = conexiones.get(0).prepareStatement(compiledQuery);
            int cantidad = 0;
            int hilo = 0;
            List<InsertThread> hilos = new ArrayList<>();

            List<PreparedStatement> lotes = new ArrayList<>();

            lotes.add(preparedStatement);


            while (resultSet.next()) {

                lotes.get(hilo).setString(1, resultSet.getString("CMTS"));
                lotes.get(hilo).setString(2, resultSet.getString("DOWN_ID"));
                lotes.get(hilo).setString(3, resultSet.getString("TOPDESC"));
                lotes.get(hilo).setString(4, resultSet.getString("MAC_NODO"));
                lotes.get(hilo).setString(5, resultSet.getString("PARENTID"));
                lotes.get(hilo).setInt(6, resultSet.getInt("TIPO"));
                lotes.get(hilo).setInt(7, resultSet.getInt("ORIGEN"));


                if (cantidad < 500000) {


                    lotes.get(hilo).addBatch();
                    cantidad++;
                } else {

                    lotes.get(hilo).addBatch();

                    InsertThread h1 = new InsertThread(lotes.get(hilo), conexiones.get(hilo), hilo);

                    conexiones.add(getOracleConnectionFromPool());

                    lotes.add(conexiones.get(hilo + 1).prepareStatement(compiledQuery));


                    hilos.add(h1);
                    hilos.get(hilo).start();

                    cantidad = 0;
                    hilo++;
                }


            }

            InsertThread h1 = new InsertThread(lotes.get(hilo), conexiones.get(hilo), hilo);
            hilos.add(h1);
            hilos.get(hilo).start();




        } catch (
                SQLException e) {
            e.printStackTrace();

            notificacionesService.enviaNotificacionGrupalPorTeams(ADMINISTRADORES, "FALLA CARGA importaInterfacesDWMETRO", e.getMessage());


        }
    }

    @Override
    public void importaBd_servassure_equiposMedicionesMETRO() {

        try (Connection  externa = getExternalMETROConnectionFromPool();){


            String compiledQuery = "INSERT INTO INFEST.BD_SERVASSURE_EQUIPOS_MEDICIONES (" +
                    " FECHA, MAC_CM, UP_ID ,DOWN_ID , MAC_ID ,BYTES_UP ,BYTES_DOWN,MAX_TXPOWER_UP,MIN_TX_POWER_UP, MAX_RX_POWER_DOWN ," +
                    " MIN_RX_POWER_DOWN, RESET_COUNT ,MAX_CER_DOWN ,MIN_CER_DOWN ,MAX_SNR_DOWN ,MIN_SNR_DOWN , MAX_UP_CER ,MAX_UP_CCER," +
                    "  MIN_SNR_UP,MAX_CCER_DOWN  , MIN_CCER_DOWN  , ORIGEN) " +
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";


            String queryExterna = "";




            System.out.println("Insertando METRO ======");
            queryExterna = "select  " +
                    "    day_stamp fecha,  " +
                    "    max_cm_desc mac_cm,  " +
                    "    max_up_id up_id,  " +
                    "    max_down_id down_id,  " +
                    "    max_mac_id mac_id,  " +
                    "    sum_sum_bytes_up/1048576 bytes_up,  " +
                    "    sum_sum_bytes_down/1048576 bytes_down,  " +
                    "    max_avg_txpower_up max_txpower_up,  " +
                    "    min_avg_txpower_up min_txpower_up,  " +
                    "    max_avg_rxpower_down max_rxpower_down,  " +
                    "    min_avg_rxpower_down min_rxpower_down,  " +
                    "    sum_sum_reset_count reset_count,  " +
                    "    max_max_cer_down max_cer_down,  " +
                    "    min_max_cer_down min_cer_down,  " +
                    "    max_min_snr_down max_snr_down,  " +
                    "    min_min_snr_down min_snr_down,  " +
                    "    max_us_cer_max max_us_cer,  " +
                    "    max_us_ccer_max max_us_ccer,  " +
                    "    min_us_snr_min min_snr,  " +
                    "    max_max_ccer_down max_ccer_down,  " +
                    "    min_max_ccer_down min_ccer_down,  " +
                    "    2 origen  " +
                    "  " +
                    "from stargus.CM_DAY_FACTS_AGG_STG  where day_stamp = trunc(sysdate)  - 1";







            Statement statement = externa.createStatement();

            ResultSet resultSet = statement.executeQuery(queryExterna);

            PreparedStatement preparedStatement;
            Connection connection = getOracleConnectionFromPool();


            List<Connection> conexiones = new ArrayList<>();
            conexiones.add(connection);

            preparedStatement = conexiones.get(0).prepareStatement(compiledQuery);
            int cantidad = 0;
            int hilo = 0;
            List<InsertThread> hilos = new ArrayList<>();

            List<PreparedStatement> lotes = new ArrayList<>();

            lotes.add(preparedStatement);


            while (resultSet.next()) {

                lotes.get(hilo).setDate(1, resultSet.getDate("FECHA"));
                lotes.get(hilo).setString(2, resultSet.getString("MAC_CM"));
                lotes.get(hilo).setInt(3, resultSet.getInt("UP_ID"));
                lotes.get(hilo).setInt(4, resultSet.getInt("DOWN_ID"));
                lotes.get(hilo).setInt(5, resultSet.getInt("MAC_ID"));
                lotes.get(hilo).setLong(6, resultSet.getLong("BYTES_UP"));
                lotes.get(hilo).setLong(7, resultSet.getLong("BYTES_DOWN"));
                lotes.get(hilo).setDouble(8, resultSet.getDouble("MAX_TXPOWER_UP"));
                lotes.get(hilo).setDouble(9, resultSet.getDouble("MIN_TXPOWER_UP"));
                lotes.get(hilo).setDouble(10, resultSet.getDouble("MAX_RXPOWER_DOWN"));
                lotes.get(hilo).setDouble(11, resultSet.getDouble("MIN_RXPOWER_DOWN"));
                lotes.get(hilo).setDouble(12, resultSet.getDouble("RESET_COUNT"));
                lotes.get(hilo).setDouble(13, resultSet.getDouble("MAX_CER_DOWN"));
                lotes.get(hilo).setDouble(14, resultSet.getDouble("MIN_CER_DOWN"));
                lotes.get(hilo).setDouble(15, resultSet.getDouble("MAX_SNR_DOWN"));
                lotes.get(hilo).setDouble(16, resultSet.getDouble("MIN_SNR_DOWN"));
                lotes.get(hilo).setDouble(17, resultSet.getDouble("MAX_US_CER"));
                lotes.get(hilo).setDouble(18, resultSet.getDouble("MAX_US_CCER"));
                lotes.get(hilo).setDouble(19, resultSet.getDouble("MIN_SNR"));
                lotes.get(hilo).setDouble(20, resultSet.getDouble("MAX_CCER_DOWN"));
                lotes.get(hilo).setDouble(21, resultSet.getDouble("MIN_CCER_DOWN"));
                lotes.get(hilo).setInt(22, resultSet.getInt("ORIGEN"));


                if (cantidad < 500000) {


                    lotes.get(hilo).addBatch();
                    cantidad++;
                } else {

                    lotes.get(hilo).addBatch();

                    InsertThread h1 = new InsertThread(lotes.get(hilo), conexiones.get(hilo), hilo);

                    conexiones.add(getOracleConnectionFromPool());

                    lotes.add(conexiones.get(hilo + 1).prepareStatement(compiledQuery));


                    hilos.add(h1);
                    hilos.get(hilo).start();

                    cantidad = 0;
                    hilo++;
                }


            }

            InsertThread h1 = new InsertThread(lotes.get(hilo), conexiones.get(hilo), hilo);
            hilos.add(h1);
            hilos.get(hilo).start();

            externa.close();


        } catch (
                SQLException e) {
            e.printStackTrace();

            notificacionesService.enviaNotificacionGrupalPorTeams(ADMINISTRADORES, "FALLA CARGA importaBd_servassure_equiposMedicionesMETRO", e.getMessage());


        }
    }

    @Override
    public void importaBd_servassure_equiposMETRO() {


        try(  Connection externa = getExternalMETROConnectionFromPool();) {


            String compiledQuery = "INSERT INTO BD_SERVASSURE_EQUIPOS (" +
                    "MAC_CM,MAC_NODO,NODO,CUENTA,DOCSIS,ORIGEN) " +
                    "VALUES (?,?,?,?,?,?)";


            String queryExterna = "";


            System.out.println("Insertando METRO ======");
            queryExterna = "SELECT distinct  " +
                    "    tn.topdesc mac_cm,  " +
                    "    td.macaddr mac_nodo,  " +
                    "    bd.transport_element_1,  " +
                    "    bd.account_number,  " +
                    "    tn.docsis_version,  " +
                    "    2 origen  " +
                    "  " +
                    "from  stargus.topology_node  tn  " +
                    "          left outer join stargus.topology_link  tl  " +
                    "                          on tl.parentid_nodetypeid = 128  " +
                    "                              and tn.topologyid = tl.topologyid  " +
                    "          left outer join stargus.topology_node tm  " +
                    "                          on tl.parentid = tm.topologyid  " +
                    "                              and tm.nodetypeid = 128  " +
                    "          left outer join stargus.topology_node  td  " +
                    "                          on td.nodetypeid = tm.nodetypeid  " +
                    "                              and td.macaddr = tm.macaddr  " +
                    "          left outer join stargus.billing_data bd  " +
                    "                          on bd.equipment_mac = tn.topdesc  " +
                    "                              and LENGTH(bd.account_number) < 11  " +
                    "                              and LENGTH(bd.transport_element_1) < 31  " +
                    "  " +
                    "where tn.nodetypeid = 2002  " +
                    "  and LENGTH(tn.topdesc) < 20  " +
                    "  and LENGTH(td.macaddr)< 20  ";

            Statement statement = externa.createStatement();

            ResultSet resultSet = statement.executeQuery(queryExterna);

            PreparedStatement preparedStatement;
            Connection connection = getOracleConnectionFromPool();


            List<Connection> conexiones = new ArrayList<>();
            conexiones.add(connection);

            preparedStatement = conexiones.get(0).prepareStatement(compiledQuery);
            int cantidad = 0;
            int hilo = 0;
            List<InsertThread> hilos = new ArrayList<>();

            List<PreparedStatement> lotes = new ArrayList<>();

            lotes.add(preparedStatement);


            while (resultSet.next()) {

                lotes.get(hilo).setString(1, resultSet.getString("MAC_CM"));
                lotes.get(hilo).setString(2, resultSet.getString("MAC_NODO"));
                lotes.get(hilo).setString(3, resultSet.getString("TRANSPORT_ELEMENT_1"));
                lotes.get(hilo).setString(4, resultSet.getString("ACCOUNT_NUMBER"));
                lotes.get(hilo).setString(5, resultSet.getString("DOCSIS_VERSION"));
                lotes.get(hilo).setInt(6, resultSet.getInt("ORIGEN"));


                if (cantidad < 500000) {


                    lotes.get(hilo).addBatch();
                    cantidad++;
                } else {

                    lotes.get(hilo).addBatch();

                    InsertThread h1 = new InsertThread(lotes.get(hilo), conexiones.get(hilo), hilo);

                    conexiones.add(getOracleConnectionFromPool());

                    lotes.add(conexiones.get(hilo + 1).prepareStatement(compiledQuery));


                    hilos.add(h1);
                    hilos.get(hilo).start();

                    cantidad = 0;
                    hilo++;
                }


            }

            InsertThread h1 = new InsertThread(lotes.get(hilo), conexiones.get(hilo), hilo);
            hilos.add(h1);
            hilos.get(hilo).start();

            externa.close();


        } catch (
                SQLException e) {
            e.printStackTrace();

            notificacionesService.enviaNotificacionGrupalPorTeams(ADMINISTRADORES, "FALLA CARGA importaBd_servassure_equiposMETRO", e.getMessage());
        }
    }

    @Override
    public void importaRuidoExternaMETRO() {


        String queryMETRO = " " +
                "                          SELECT distinct\n" +
//                "                          --Modificar cuando sean dias atrazados\n" +
                "                            TRUNC(SYSDATE)-1 AS FECHA_LECTURA,\n" +
                "                            uphf.UP_ID,\n" +
                "                            tnMarket.TOPNAME AS MARKET,\n" +
                "                            tnHUB.TOPNAME AS HUB,  \n" +
                "                            tnCMTS.TOPNAME AS CMTS,\n" +
                "                            tnMAC.TOPNAME AS DOMAIN,  \n" +
                "                            tnMAC.TOPDESC AS US_MAC,\n" +
                "                            uphf.UP_DESC AS US_DESC,  \n" +
                "                            COUNT(uphf.UP_ID) AS MUESTRAS,\n" +
                "                            SUM(CASE WHEN uphf.MIN_SNR<30 THEN 1 ELSE 0 END) AS snr30,\n" +
                "                            ROUND(SUM(CASE WHEN uphf.MIN_SNR<30 THEN 1 ELSE 0 END)/COUNT(uphf.UP_ID),3) AS PCT_SNR30,\n" +
                "                            MIN(uphf.MIN_SNR) AS MIN_SNR,\n" +
                "                            ROUND(AVG(uphf.MIN_SNR),1) AS AVG_SNR,\n" +
                "                            ROUND(STDDEV(uphf.MIN_SNR),1) AS DESVEST_SNR,\n" +
                "                            ROUND(AVG(uphf.MAX_UNIQUE_CMS),1) AS AVG_UNIQUE_CM,\n" +
                "                            MAX(uphf.MAX_CER) as MAX_CER,\n" +
                "                            MAX(uphf.MAX_PERCENT_UTIL) AS US_UTILIZATION,\n" +
                "                            CASE \n" +
                "                              WHEN (COUNT(uphf.UP_ID)/168)<0.2 THEN 'Muestras Insuficientes'\n" +
                "                            ELSE\n" +
                "                              CASE\n" +
                "                                WHEN ROUND(AVG(uphf.MAX_UNIQUE_CMS),1) = 0 THEN 'Sin Equipos'\n" +
                "                              ELSE\n" +
                "                                CASE\n" +
                "                                  WHEN ROUND(SUM(CASE WHEN uphf.MIN_SNR<30 THEN 1 ELSE 0 END)/COUNT(uphf.UP_ID),3)>0.4 THEN 'Mayor a 40'\n" +
                "                                  WHEN ROUND(SUM(CASE WHEN uphf.MIN_SNR<30 THEN 1 ELSE 0 END)/COUNT(uphf.UP_ID),3)>0.25 THEN 'Mayor a 25'\n" +
                "                                  WHEN ROUND(SUM(CASE WHEN uphf.MIN_SNR<30 THEN 1 ELSE 0 END)/COUNT(uphf.UP_ID),3)>0.15 THEN 'Mayor a 15'\n" +
                "                                  WHEN ROUND(SUM(CASE WHEN uphf.MIN_SNR<30 THEN 1 ELSE 0 END)/COUNT(uphf.UP_ID),3)>0.1 THEN 'Mayor a 10'\n" +
                "                              ELSE\n" +
                "                                  'Menor a 10'\n" +
                "                                  END\n" +
                "                                END\n" +
                "                            END AS Resultado_SNR,\n" +
                "                            --Modificar cuando sean dias atrazados\n" +
                "                              SUM(CASE WHEN uphf.MIN_SNR<30 AND trunc(uphf.HOUR_STAMP) = trunc(sysdate)-1 THEN 1 ELSE 0 end) AS Resultado_SNR24\n" +
                "                           -- SUM(CASE WHEN (uphf.MIN_SNR<30 AND uphf.HOUR_STAMP between trunc(sysdate)-1 and trunc(sysdate) )THEN 1 ELSE 0 END) AS Resultado_SNR24\n" +
                "                               \n" +
                "                          FROM STARGUS.UPSTREAM_HOUR_FACTS uphf\n" +
                "                            \n" +
                "                          LEFT OUTER JOIN STARGUS.TOPOLOGY_NODE tnMAC\n" +
                "                            ON uphf.MAC_ID = tnMAC.TOPOLOGYID\n" +
                "                            \n" +
                "                          LEFT OUTER JOIN STARGUS.TOPOLOGY_NODE tnCMTS\n" +
                "                            ON uphf.CMTS_ID = tnCMTS.TOPOLOGYID\n" +
                "\n" +
                "                          LEFT OUTER JOIN STARGUS.TOPOLOGY_LINK tlHUB\n" +
                "                            ON uphf.CMTS_ID = tlHUB.TOPOLOGYID\n" +
                "                            AND tlHUB.PARENTID_NODETYPEID = 1002\n" +
                "                            \n" +
                "                          LEFT OUTER JOIN STARGUS.TOPOLOGY_NODE tnHUB\n" +
                "                            ON tnHUB.TOPOLOGYID = tlHUB.PARENTID\n" +
                "                            \n" +
                "                          LEFT OUTER JOIN STARGUS.TOPOLOGY_LINK tlMarket\n" +
                "                            ON tnHUB.TOPOLOGYID = tlMarket.TOPOLOGYID\n" +
                "                            AND tlMarket.PARENTID_NODETYPEID = 1001\n" +
                "                              \n" +
                "                          LEFT OUTER JOIN STARGUS.TOPOLOGY_NODE tnMarket\n" +
                "                            ON tnMarket.TOPOLOGYID = tlMarket.PARENTID  \n" +
                "                            \n" +
                "                          WHERE \n" +
//                "                          --Modificar cuando sean dias atrazados\n" +
                "                            uphf.HOUR_STAMP >= TRUNC(SYSDATE)-7 AND uphf.HOUR_STAMP < TRUNC(SYSDATE)\n" +
                "                            AND uphf.UP_ID NOT IN ('555098132','555098133','555098134','555098135','555105263','555105264','555105277','555105278','560000511','560000503')\n" +
                "                              \n" +
                "                          GROUP BY\n" +
//                "                          --Modificar cuando sean dias atrazados\n" +
                "                            TRUNC(SYSDATE)-1,\n" +
                "                            uphf.UP_ID,\n" +
                "                            tnMarket.TOPNAME,\n" +
                "                            tnHUB.TOPNAME,  \n" +
                "                            tnCMTS.TOPNAME,\n" +
                "                            tnMAC.TOPNAME,  \n" +
                "                            tnMAC.TOPDESC,\n" +
                "                            uphf.UP_DESC\n" +
                "";


        String compiledQuery = "INSERT INTO infest.BD_SAA_SNR_UP " +
                "( FECHA, " +
                "    UP_ID,         " +
                "    MARKET ,  " +
                "    HUB ,    " +
                "    CMTS  , " +
                "    DOMAIN  ,  " +
                "    US_MAC   , " +
                "    US_DESC   ,  " +
                "    MUESTRAS  ," +
                "    SNR30     , " +
                "    PCT_SNR30 , " +
                "    MIN_SNR   , " +
                "    AVG_SNR   ,  " +
                "    DESVEST_SNR,   " +
                "    AVG_UNIQUE_CM, " +
                "    MAX_CER     ,    " +
                "    US_UTILIZATION,  " +
                "    RESULTADO_SNR,   " +
                "    RESULTADO_SNR24 ) " +
                "VALUES (?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?) ";

        try (Connection connection = getExternalMETROConnectionFromPool();) {


            System.out.println("INSERTANDO DE METRO");
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryMETRO);


            Connection infest = getOracleConnectionFromPool();


//            rutinaDao.truncateOrdenesCompletadas();

            List<Connection> conexiones = new ArrayList<>();
            conexiones.add(infest);


            PreparedStatement preparedStatement = conexiones.get(0).prepareStatement(compiledQuery);


            int cantidad = 0;
            int hilo = 0;
            List<InsertThread> hilos = new ArrayList<>();

            List<PreparedStatement> lotes = new ArrayList<>();

            lotes.add(preparedStatement);


            while (resultSet.next()) {


                lotes.get(hilo).setDate(1, resultSet.getDate("FECHA_LECTURA"));
                lotes.get(hilo).setString(2, resultSet.getString("UP_ID"));
                lotes.get(hilo).setString(3, resultSet.getString("MARKET"));
                lotes.get(hilo).setString(4, resultSet.getString("HUB"));
                lotes.get(hilo).setString(5, resultSet.getString("CMTS"));
                lotes.get(hilo).setString(6, resultSet.getString("DOMAIN"));
                lotes.get(hilo).setString(7, resultSet.getString("US_MAC"));
                lotes.get(hilo).setString(8, resultSet.getString("US_DESC"));


                lotes.get(hilo).setInt(9, resultSet.getInt("MUESTRAS"));
                lotes.get(hilo).setInt(10, resultSet.getInt("SNR30"));


                lotes.get(hilo).setDouble(11, resultSet.getDouble("PCT_SNR30"));
                lotes.get(hilo).setDouble(12, resultSet.getDouble("MIN_SNR"));
                lotes.get(hilo).setDouble(13, resultSet.getDouble("AVG_SNR"));
                lotes.get(hilo).setDouble(14, resultSet.getDouble("DESVEST_SNR"));
                lotes.get(hilo).setDouble(15, resultSet.getDouble("AVG_UNIQUE_CM"));
                lotes.get(hilo).setDouble(16, resultSet.getDouble("MAX_CER"));
                lotes.get(hilo).setDouble(17, resultSet.getDouble("US_UTILIZATION"));


                lotes.get(hilo).setString(18, resultSet.getString("RESULTADO_SNR"));

                lotes.get(hilo).setInt(19, resultSet.getInt("SNR30"));


                if (cantidad < 500000) {


                    lotes.get(hilo).addBatch();
                    cantidad++;
                } else {

                    lotes.get(hilo).addBatch();

                    InsertThread h1 = new InsertThread(lotes.get(hilo), conexiones.get(hilo), hilo);

                    conexiones.add(getOracleConnectionFromPool());

                    lotes.add(conexiones.get(hilo + 1).prepareStatement(compiledQuery));


                    hilos.add(h1);
                    hilos.get(hilo).start();

                    cantidad = 0;
                    hilo++;
                }


            }

            InsertThread h1 = new InsertThread(lotes.get(hilo), conexiones.get(hilo), hilo);
            hilos.add(h1);
            hilos.get(hilo).start();


            resultSet.close();
            statement.close();


        } catch (Exception e) {
            e.printStackTrace();

            notificacionesService.enviaNotificacionGrupalPorTeams(ADMINISTRADORES, "FALLA CARGA importaRuidoExternaMETRO", e.getMessage());

        }

    }

    @Override
    public void externadeotroorigen() {
        String query = "select * from usuario where idusuario = 719 ";


        try(Connection connection = getOracleConnectionFromPool();){

            Statement statement  = connection.createStatement();

            ResultSet resultSet = statement.executeQuery(query);

            if (resultSet.next()){
                System.out.println(resultSet.getString("USUARIO")+" desde el primero======================================");
            }

        } catch (SQLException e) {
           e.printStackTrace();
        }

        try(Connection connection = getExternalMETROConnectionFromPool();){

            Statement statement  = connection.createStatement();

            ResultSet resultSet = statement.executeQuery(query);

            if (resultSet.next()){
                System.out.println(resultSet.getString("USUARIO")+" desde el segundo ======================================");
            }

        } catch (SQLException e) {
           e.printStackTrace();
        }

    }



}
