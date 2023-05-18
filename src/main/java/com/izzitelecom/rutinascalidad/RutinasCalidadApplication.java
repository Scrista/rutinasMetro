package com.izzitelecom.rutinascalidad;

import com.izzitelecom.rutinascalidad.Service.RutinaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
public class RutinasCalidadApplication {

    @Autowired
    RutinaService rutinaService;

    public static void main(String[] args) {
        SpringApplication.run(RutinasCalidadApplication.class, args);
    }

//    @Scheduled(fixedRate = 1000)
@Scheduled(cron = "0 45 3 1/1 * ?")
    public void importaUtilizacionUPCALIDADMETRO() {
        rutinaService.importaUtilizacionUPCALIDADMETRO();
    }


    @Scheduled(cron = "0 25 5 1/1 * ?")
    public void importaInterfacesDWMETRO(){
        System.out.println("========Inicio importaInterfacesDW========");



            rutinaService.importaInterfacesDWMETRO();



        System.out.println("========fin importaInterfacesDW========");
    }


    @Scheduled(cron = "0 25 7 1/1 * ?")
    public void importaBd_servassure_equiposMedicionesMETRO(){
        System.out.println("========Inicio importaBd_servassure_equiposMediciones========");



        rutinaService.importaBd_servassure_equiposMedicionesMETRO();




    }


    @Scheduled(cron = "0 20 6 1/1 * ?")
    public void importaBd_servassure_equiposMETRO(){
        System.out.println("========Inicio importaBd_servassure_equipos========");



            rutinaService.importaBd_servassure_equiposMETRO();



        System.out.println("========fin importaBd_servassure_equipos========");
    }

    @Scheduled(cron = "0 30 4 1/1 * ?")
    public void importaUTILIZACIONDSExternaMETRO(){
        System.out.println("========Inicio importaUTILIZACIONDSExterna========");



            rutinaService.importaUTILIZACIONDSMETRO();



        System.out.println("========fin importaUTILIZACIONDSExterna========");
    }


    @Scheduled(cron = "0 0 3 1/1 * ?")
    public void importaRuidoExternaMETRO(){
        System.out.println("========Inicio importaRuidoExterna========");



            rutinaService.importaRuidoExternaMETRO();



        System.out.println("========fin importaRuidoExterna========");
    }

}
