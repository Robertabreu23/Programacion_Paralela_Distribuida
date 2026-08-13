package com.universidad.semana14;

import java.util.Map;                                    // Respuesta del servicio de actores

/**
 * DEMO LOCAL (sin AWS)
 * -----------------------------------------------------------------------------
 * Ejecuta una bateria de tareas contra el pool de actores, incluida una que
 * falla a proposito, para demostrar en los logs que el supervisor reinicia al
 * worker y que el sistema sigue atendiendo peticiones despues del fallo.
 *
 * Uso:  java -jar target/actor-serverless.jar
 */
public class LocalDemo {

    public static void main(String[] args) throws Exception {
        System.out.println("\n=== DEMO: Modelo de Actores con supervision ===\n");

        // Casos de prueba: {operacion, carga util}
        String[][] casos = {
                {"sum", "10,20,30,40"},                  // Suma numeros -> 100
                {"upper", "arquitectura serverless"},    // Mayusculas
                {"reverse", "actores"},                  // Invierte texto
                {"fail", "provoca-error"},               // FALLO INTENCIONADO -> supervisor reinicia el worker
                {"sum", "1,2,3"},                        // El sistema sigue vivo tras el fallo
                {"upper", "sistema tolerante a fallos"}, // Confirmacion de recuperacion
                {"raiz", "9"}                            // Operacion no soportada -> error controlado
        };

        for (String[] caso : casos) {                    // Recorremos los casos uno a uno
            Map<String, Object> r = ActorService.process(caso[0], caso[1]); // Enviamos la tarea al supervisor
            System.out.println(">> PETICION  : " + caso[0] + "('" + caso[1] + "')");
            System.out.println("<< RESPUESTA : " + r);   // Imprimimos la respuesta completa
            System.out.println();
            Thread.sleep(500);                           // Pausa para que los logs de reinicio se vean ordenados
        }

        System.out.println("=== FIN DE LA DEMO: el sistema respondio a TODAS las peticiones ===");
        ActorService.system().terminate();               // Cerramos el ActorSystem ordenadamente
    }
}
