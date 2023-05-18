package com.izzitelecom.rutinascalidad.Service.impl;


import com.izzitelecom.rutinascalidad.Service.NotificacionesService;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;

@Service
public class NotificacionesServiceImpl implements NotificacionesService {

//        final String  rutaServicio="http://localhost:8080/";
    final String rutaServicio = "http://gestopdb.izzi.mx:8080/izziTeams/";

    //    final String  rutaServicio="https://gestop.izzi.mx:7105/izziTeams/";
    @Override
    public String enviaNotificacionPorTeams(String mensaje, int idFiscalizacionH, String destinatarios, String rutaArchivo) {
        MultiValueMap<String, Object> values = new LinkedMultiValueMap<String, Object>();
        System.out.println("==========enviando teams=========");


        String rest = rutaServicio + "subirArchivoHallazgoDrive";
        String rutaCompartida = rutaArchivo;
        System.out.println("==========archivo cargado=========");
        String msg = mensaje + "<h3> para descargar la evidencia haga click <a href='" + rutaCompartida + "'> aquí </a></h3>";
        values = new LinkedMultiValueMap<String, Object>();
        values.add("mensaje", msg);
        values.add("destino", destinatarios);
        values.add("ruta", rutaCompartida);

        rest = rutaServicio + "mandaAlertaGrupal";


        try {
            String rutaReporte = CustomRestTemplate().postForObject(rest, values, String.class);
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }


        System.out.println("========== team enviado=========");
        return "ok";
    }

    @Override
    public String subeArchivoDrive(int idFiscalizacionH, String rutaReporte) {
        MultiValueMap<String, Object> values = new LinkedMultiValueMap<String, Object>();
        System.out.println("==========enviando teams=========");
        try {


            String filePath = rutaReporte;

            File f = new File(filePath);
            InputStream inputStream = new FileInputStream(f);

            byte[] frd = Files.readAllBytes(Paths.get(filePath));
            String dec = new String(frd);


            byte[] input_file = Files.readAllBytes(Paths.get(filePath));

            byte[] encodedBytes = Base64.getEncoder().encode(input_file);
            String encodedString = new String(encodedBytes);

            values.add("archivo", encodedString);
            values.add("tamaniFile", f.length());

        } catch (Exception e) {
            e.printStackTrace();
        }

        values.add("nombreFile", "evidenciaHAllazgo" + idFiscalizacionH + ".pdf");
        values.add("destino", "");
        values.add("idHallazgo", idFiscalizacionH);


        String rest = rutaServicio + "subirArchivoHallazgoDrive";
        String rutaCompartida = "";
        try {
            rutaCompartida = CustomRestTemplate().postForObject(rest, values, String.class);
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }


        return rutaCompartida;

    }

    @Override
    public String subeArchivoDriveSolucion(int idHallazgo, String adjuntos, int i) {
        MultiValueMap<String, Object> values = new LinkedMultiValueMap<String, Object>();
        System.out.println("==========enviando teams=========");
        String evidencias[] = adjuntos.split(",");




            try {


                String filePath = adjuntos;

                File f = new File(filePath);
                InputStream inputStream = new FileInputStream(f);

                byte[] frd = Files.readAllBytes(Paths.get(filePath));
                String dec = new String(frd);


                byte[] input_file = Files.readAllBytes(Paths.get(filePath));

                byte[] encodedBytes = Base64.getEncoder().encode(input_file);
                String encodedString = new String(encodedBytes);

                values.add("archivo", encodedString);
                values.add("tamaniFile", f.length());

            } catch (Exception e) {
                e.printStackTrace();
            }

            values.add("nombreFile", "evidenciaCierreHAllazgo" + i + ".pdf");
            values.add("destino", "");
            values.add("idHallazgo", idHallazgo);


            String rest = rutaServicio + "subirArchivoHallazgoDrive";

            String rutaCompartida="";
            try {
                rutaCompartida += CustomRestTemplate().postForObject(rest, values, String.class)+",";
            } catch (KeyStoreException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (KeyManagementException e) {
                e.printStackTrace();
            }


        return rutaCompartida;
    }

    @Override
    public String enviaNotificacionPorTeamsCierre(String notificacion, int idHallazgo, String adm, List<String> rutaCompartida) {




        MultiValueMap<String, Object> values = new LinkedMultiValueMap<String, Object>();
        System.out.println("==========enviando teams=========");

        for (String ruta : rutaCompartida){
            System.out.println("ruta compartida de "+idHallazgo);
            System.out.println(ruta);
        }


            String rest = rutaServicio + "subirArchivoHallazgoDrive";

            System.out.println("==========archivo cargado=========");
            String msg = notificacion + "<h3>  <a href='" + rutaCompartida.get(1).replace(",","") + "'> hallazgo  </a></h3>";
             msg +=  "<h3>  <a href='" +rutaCompartida.get(0).replace(",","") + "'> solucion </a></h3>";
            values = new LinkedMultiValueMap<String, Object>();
            values.add("mensaje", msg);
            values.add("destino", adm);
            values.add("ruta", rutaCompartida);

            rest = rutaServicio + "mandaAlerta";


            try {
                String rutaReporte = CustomRestTemplate().postForObject(rest, values, String.class);
            } catch (KeyStoreException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (KeyManagementException e) {
                e.printStackTrace();
            }




        System.out.println("========== team enviado=========");
        return "ok";
    }

    @Override
    public void enviaNotificacionPorTeamsHerramientas(String mensaje, int idFiscH, String adm, String rutaArchivo) {
        MultiValueMap<String, Object> values = new LinkedMultiValueMap<String, Object>();
        System.out.println("==========enviando teams=========");


        String rest = rutaServicio + "subirArchivoHallazgoDrive";
        String rutaCompartida = rutaArchivo;
        System.out.println("==========archivo cargado=========");
        String msg = mensaje + "<h3> para descargar la evidencia haga click <a href='" + rutaCompartida + "'> aquí </a></h3>";
        values = new LinkedMultiValueMap<String, Object>();
        values.add("mensaje", msg);
        values.add("destino", adm);
        values.add("ruta", rutaCompartida);

        rest = rutaServicio + "mandaAlerta";


        try {
            String rutaReporte = CustomRestTemplate().postForObject(rest, values, String.class);
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }


        System.out.println("========== team de herramientas enviado=========");

    }

    @Override
    public String subeArchivoDriveHerramientas(int idFiscH, String rutaReporte) {
        MultiValueMap<String, Object> values = new LinkedMultiValueMap<String, Object>();
        System.out.println("==========enviando teams=========");
        try {


            String filePath = rutaReporte;

            File f = new File(filePath);
            InputStream inputStream = new FileInputStream(f);

            byte[] frd = Files.readAllBytes(Paths.get(filePath));
            String dec = new String(frd);


            byte[] input_file = Files.readAllBytes(Paths.get(filePath));

            byte[] encodedBytes = Base64.getEncoder().encode(input_file);
            String encodedString = new String(encodedBytes);

            values.add("archivo", encodedString);
            values.add("tamaniFile", f.length());

        } catch (Exception e) {
            e.printStackTrace();
        }

        values.add("nombreFile", "evidenciaHallazgoHerramientas" + idFiscH + ".pdf");
        values.add("destino", "");
        values.add("idHallazgo", idFiscH);


        String rest = rutaServicio + "subirArchivoHallazgoDriveHerramientas";
        String rutaCompartida = "";
        try {
            rutaCompartida = CustomRestTemplate().postForObject(rest, values, String.class);
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }


        return rutaCompartida;
    }

    @Override
    public String subeArchivoDriveFallaVentas(String folio, String rutaReporte) {
        MultiValueMap<String, Object> values = new LinkedMultiValueMap<String, Object>();
        System.out.println("==========enviando teams=========");
        try {


            String filePath = rutaReporte;

            File f = new File(filePath);
            InputStream inputStream = new FileInputStream(f);

            byte[] frd = Files.readAllBytes(Paths.get(filePath));
            String dec = new String(frd);


            byte[] input_file = Files.readAllBytes(Paths.get(filePath));

            byte[] encodedBytes = Base64.getEncoder().encode(input_file);
            String encodedString = new String(encodedBytes);

            values.add("archivo", encodedString);
            values.add("tamaniFile", f.length());

        } catch (Exception e) {
            e.printStackTrace();
        }

        values.add("nombreFile", folio+".jpg");
        values.add("destino", "");
        values.add("folio", folio);


        String rest = rutaServicio + "subirArchivoVentasDrive";
        String rutaCompartida = "";
        try {
            rutaCompartida = CustomRestTemplate().postForObject(rest, values, String.class);
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }


        return rutaCompartida;
    }

    @Override
    public void enviaNotificacionPorTeamsFallaVentas(String mensajeTeams, String dm, String rutaArchivo) {

        MultiValueMap<String, Object> values = new LinkedMultiValueMap<String, Object>();
        System.out.println("==========enviando teams=========");


        String rest = rutaServicio + "subirArchivoHallazgoDrive";
        String rutaCompartida = rutaArchivo;
        System.out.println("==========archivo cargado=========");
        String msg = mensajeTeams +( rutaArchivo.equals("") ? "" : "<h3> para descargar la evidencia haga click <a href='" + rutaCompartida + "'> aquí </a></h3>");
        values = new LinkedMultiValueMap<String, Object>();
        values.add("mensaje", msg);
        values.add("destino", dm);
        values.add("ruta", rutaCompartida);

        rest = rutaServicio + "mandaAlerta";


        try {
            String rutaReporte = CustomRestTemplate().postForObject(rest, values, String.class);
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }


        System.out.println("========== team de ventas enviado=========");
    }

    @Override
    public void enviaNotificacionGrupalPorTeams(String administradores, String titulo, String message) {
        MultiValueMap<String, Object> values = new LinkedMultiValueMap<String, Object>();
        System.out.println("==========enviando teams=========");


        String rest = rutaServicio + "subirArchivoHallazgoDrive";
        String rutaCompartida = "";
        System.out.println("==========archivo cargado=========");
        String msg = "<div>" +
                "<h4>"+titulo+"</h4>"+
                "<p>"+message+"</p>"+
                " </div>";
        values = new LinkedMultiValueMap<String, Object>();
        values.add("mensaje", msg);
        values.add("destino", administradores);
        values.add("ruta", rutaCompartida);

        rest = rutaServicio + "mandaAlertaGrupal";


        try {
            String rutaReporte = CustomRestTemplate().postForObject(rest, values, String.class);
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }

    }


    private RestTemplate CustomRestTemplate() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
            @Override
            public boolean isTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
                return true;
            }
        };
        SSLContext sslContext = org.apache.http.ssl.SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
        SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext, new NoopHostnameVerifier());
        CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(csf).build();
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setHttpClient(httpClient);
        RestTemplate restTemplate = new RestTemplate(requestFactory);
        return restTemplate;
    }
}
