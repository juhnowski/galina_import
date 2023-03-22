package galina;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class App {
    ArrayList<Pair> input = new ArrayList<>();

    public class Pair{
        String key;
        String value;
        public Pair(String key, String value){
            this.key = key;
            this.value = value;
        }
    }

    public class Observ {
        public String meteodate;
        public String meteotime;
        public long id;
        public String fieldname;
        public String value;

        public static String[] years = {"08","09","10","11","12","13","14","15","16","17","18","19","20","21","22"};
        
        public Observ(
            String meteodate,
            String fieldname,
            String value
        ) {
            this.meteodate = meteodate;
            this.fieldname = fieldname;
            this.value = value;
            this.meteotime = "12:00";
            createId();
        }

        private void createId(){
            LocalDate parsedDate = LocalDate.parse(meteodate, DateTimeFormatter.ofPattern("dd.MM.yy"));
            LocalTime parsedTime = LocalTime.parse(meteotime, DateTimeFormatter.ofPattern("HH:mm"));
            id = LocalDateTime.of(parsedDate, parsedTime)
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();
        }

        // public String toSelect() {
        //     return "SELECT INTO public.observ (id, MeteoDate, MeteoTime, "+fieldname+") VALUES ("+id+", "+meteodate+", "+meteotime+", "+value+");";
        // }

        // public String toInsert() {
        //     return "INSERT INTO public.observ (id, MeteoDate, MeteoTime, "+fieldname+") VALUES ("+id+", "+meteodate+", "+meteotime+", "+value+");";
        // }

        // public String toUpdate() {
        //     return "UPDATE public.observ SET " + fieldname + " = "+value+" WHERE id = "+id+";";
        // }

        public String toSQL() {
            return "UPDATE public.observ SET " + fieldname + " = "+value+" WHERE id = "+id+";" +
            "INSERT INTO public.observ (id, MeteoDate, MeteoTime, "+fieldname+") VALUES ("+id+", "+meteodate+", "+meteotime+", "+value+");";
                   
        }

/*
 * WITH upsert AS (
     UPDATE public.observ  SET average_snow_density=400.23 
     WHERE id=1654196400000 
     RETURNING *
)
INSERT INTO public.observ (id, average_snow_density) 
SELECT 1654196400000, 400.23 
WHERE NOT EXISTS (SELECT * FROM upsert)
 */
    }

    public void init(){
        input.add(new Pair("VishenskoeWaterColor","/home/ilya/galina_import/data/Bol_cvet.csv"));
        input.add(new Pair("VishenskoeElConduct","/home/ilya/galina_import/data/Bol_El.csv"));
        input.add(new Pair("VishenskoepH","/home/ilya/galina_import/data/Bol_pH.csv"));
        input.add(new Pair("VishenskoeTemp","/home/ilya/galina_import/data/Bol_Temp.csv"));
        input.add(new Pair("VishenskoeWaterLevel","/home/ilya/galina_import/data/Bol_Uroven.csv"));
        input.add(new Pair("KalachikWaterColor","/home/ilya/galina_import/data/Kalachik_cvet.csv"));
        input.add(new Pair("KalachikElConduct","/home/ilya/galina_import/data/Kalachik_El.csv"));
        input.add(new Pair("KalachikpH","/home/ilya/galina_import/data/Kalachik_pH.csv"));
        input.add(new Pair("KalachikTemp","/home/ilya/galina_import/data/Kalachik_Temp.csv"));
        input.add(new Pair("KalachikWaterLevel","/home/ilya/galina_import/data/Kalachik_Uroven.csv"));
        input.add(new Pair("KerzhenetsWaterColor","/home/ilya/galina_import/data/Kerzhenets_cvet.csv"));
        input.add(new Pair("KerzhenetsElConduct","/home/ilya/galina_import/data/Kerzhenets_El.csv"));
        input.add(new Pair("KerzhenetspH","/home/ilya/galina_import/data/Kerzhenets_pH.csv"));
        input.add(new Pair("KerzhenetsTemp","/home/ilya/galina_import/data/Kerzhenets_Temp.csv"));
        input.add(new Pair("KerzhenetsWaterLevel","/home/ilya/galina_import/data/Kerzhenets_Uroven.csv"));
        input.add(new Pair("KrugloeWaterColor","/home/ilya/galina_import/data/Krugloe_cvet.csv"));
        input.add(new Pair("KrugloeElConduct","/home/ilya/galina_import/data/Krugloe_El.csv"));
        input.add(new Pair("KrugloepH","/home/ilya/galina_import/data/Krugloe_pH.csv"));
        input.add(new Pair("KrugloeTemp","/home/ilya/galina_import/data/Krugloe_Temp.csv"));
        input.add(new Pair("KrugloeWaterLevel","/home/ilya/galina_import/data/Krugloe_Uroven.csv"));
        input.add(new Pair("MakhovskoeWaterColor","/home/ilya/galina_import/data/Makhovskoe_cvet.csv"));
        input.add(new Pair("MakhovskoeElConduct","/home/ilya/galina_import/data/Makhovskoe_El.csv"));
        input.add(new Pair("MakhovskoepH","/home/ilya/galina_import/data/Makhovskoe_pH.csv"));
        input.add(new Pair("MakhovskoeTemp","/home/ilya/galina_import/data/Makhovskoe_Temp.csv"));
        input.add(new Pair("MakhovskoeWaterLevel","/home/ilya/galina_import/data/Makhovskoe_Uroven.csv"));
        input.add(new Pair("NRustayskoyeWaterColor","/home/ilya/galina_import/data/NRustayskoye_cvet.csv"));
        input.add(new Pair("NRustayskoyeElConduct","/home/ilya/galina_import/data/NRustayskoye_El.csv"));
        input.add(new Pair("NRustayskoyepH", "/home/ilya/galina_import/data/NRustayskoye_pH.csv"));
        input.add(new Pair("NRustayskoyeTemp","/home/ilya/galina_import/data/NRustayskoye_Temp.csv"));
        input.add(new Pair("/home/ilya/galina_import/data/Makhovskoe_Uroven.csv","MakhovskoeWaterLevel"));
        input.add(new Pair("VishnyaWaterColor","/home/ilya/galina_import/data/Vishnya_cvet.csv"));
        input.add(new Pair("VishnyaElConduct","/home/ilya/galina_import/data/Vishnya_El.csv"));
        input.add(new Pair("VishnyapH","/home/ilya/galina_import/data/Vishnya_pH.csv"));
        input.add(new Pair("VishnyaTemp","/home/ilya/galina_import/data/Vishnya_Temp.csv"));
        input.add(new Pair("VishnyaWaterLevel","/home/ilya/galina_import/data/Vishnya_Uroven.csv"));
        input.add(new Pair("WellWaterColor","/home/ilya/galina_import/data/Well_cvet.csv"));
        input.add(new Pair("WellElConduct","/home/ilya/galina_import/data/Well_El.csv"));
        input.add(new Pair("WellpH","/home/ilya/galina_import/data/Well_pH.csv"));
        input.add(new Pair("WellTemp","/home/ilya/galina_import/data/Well_Temp.csv"));
        input.add(new Pair("WellWaterLevel","/home/ilya/galina_import/data/Well_Uroven.csv"));
    }

    public void run(){
        String DB_URL     = "jdbc:postgresql://rc1b-h2ibywz9kbvpjomr.mdb.yandexcloud.net:6432/meteo?targetServerType=master&ssl=true&sslmode=verify-full";
        String DB_USER    = "meteo";
        String DB_PASS    = "meteometeo";
        Class.forName("org.postgresql.Driver");

        input.parallelStream().forEach(p -> {

            try (FileReader fr = new FileReader(p.value);
                 BufferedReader br = new BufferedReader(fr);
                 Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);) {
                String line ="";
                while (line!=null) {
                    line = br.readLine();
                    if (line!=null){
                        String[] values=line.split(";");
                        for (int i=0; i<Observ.years.length-1; i++){
                            if (!values[i+1].isEmpty()) {
                                Observ o = new Observ(values[0].substring(0,5)+Observ.years[i],
                                p.key,
                                values[i+1]);
                                
                                ResultSet q = conn.createStatement().executeQuery("SELECT version()");
                                if(q.next()) {System.out.println(q.getString(1));}
                            }
                        }                        
                    } 
                }
                
            } catch (Exception e){
                e.printStackTrace();
            }


        });
    };
    public static void main(String[] args) {

        
        App a = new App();
        a.init();
        





            
        // try {
        //     Class.forName("org.postgresql.Driver");

        //     Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
        //     ResultSet q = conn.createStatement().executeQuery("SELECT version()");
        //     if(q.next()) {System.out.println(q.getString(1));}

        //     conn.close();
        // }
        // catch(Exception ex) {ex.printStackTrace();}
    }
}
