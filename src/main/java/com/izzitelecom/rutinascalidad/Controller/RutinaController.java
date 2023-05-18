package com.izzitelecom.rutinascalidad.Controller;

import com.izzitelecom.rutinascalidad.Service.RutinaService;
import com.izzitelecom.rutinascalidad.config.DataSourceConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.sql.SQLException;

@Controller
public class RutinaController {
    @Autowired
    RutinaService rutinaService;

    @RequestMapping(value = "ejecutarRutina/{id}")//, method = RequestMethod.POST, produces = "application/json")
    @ResponseBody
    public String testRepo(HttpServletRequest request, @PathVariable Integer id) throws Exception {

        switch (id) {

            case 1:
                System.out.println("importaInterfacesDW");
                rutinaService.importaInterfacesDWMETRO();//
                // rutinaService.crearReporteFiscalesMETRO();
                break;
            case 2:
                System.out.println("importaEQUIPOS");
                rutinaService.importaBd_servassure_equiposMETRO();
                //rutinaService.ActualizarBdGeneradasMETRO();
                break;

            case 3:
                System.out.println("importa Bd_servassure_equiposMediciones");
                rutinaService.importaBd_servassure_equiposMedicionesMETRO();
                // rutinaService.importaBd_servassure_equiposMETRO();
                break;


            case 4:
                System.out.println("importaRuidoExterna");
                rutinaService.importaRuidoExternaMETRO();
                break;

            case 5:
                System.out.println("importaUtilizacionUPCALIDAD");
                rutinaService.importaUtilizacionUPCALIDADMETRO();
                break;

            case 6:
                System.out.println("importaUTILIZACIONDSOPULIDO");
                rutinaService.importaUTILIZACIONDSMETRO();
                break;

            case 7:
                System.out.println("prueba de conexion externa");
                rutinaService.externadeotroorigen();
                break;


        }
        return "index";
    }


}
