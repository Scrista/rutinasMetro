package scheduled;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Configuration
@EnableScheduling
@Component
public class rutinaCalidad {


@Scheduled(fixedRate = 1000)
    public void rutina_SP_DESACTIVARANUNCIOINDEX() {
        System.out.println("rutinaaaa");
    }
}
