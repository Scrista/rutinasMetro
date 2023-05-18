package com.izzitelecom.rutinascalidad.Service;

import java.util.List;

public interface NotificacionesService {
    String enviaNotificacionPorTeams(String mensaje, int idFiscalizacionH, String destinatarios, String rutaReporte);

     String subeArchivoDrive(int idFiscalizacionH, String rutaReporte);

    String subeArchivoDriveSolucion(int idHallazgo, String adjuntos, int i);

    String enviaNotificacionPorTeamsCierre(String notificacion, int idHallazgo, String adm, List<String> rutaCompartida);

    void enviaNotificacionPorTeamsHerramientas(String mensaje, int idFiscH, String adm, String rutaCompartida);

    String subeArchivoDriveHerramientas(int idFiscH, String rutaReporte);

    String subeArchivoDriveFallaVentas(String folio, String home);

    void enviaNotificacionPorTeamsFallaVentas(String mensajeTeams, String dm, String rutaCompartida);

    void enviaNotificacionGrupalPorTeams(String administradores, String titulo, String message);
}
