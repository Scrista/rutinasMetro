package com.izzitelecom.rutinascalidad.Controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class testController {

    @GetMapping("test")
    @ResponseBody
    public String hello(){
        return "ok";
    }
}
