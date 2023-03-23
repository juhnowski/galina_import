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
        int year;
        public Pair(String key, String value, int year){
            this.key = key;
            this.value = value;
            this.year = year;
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
            this.meteotime = "00:00";
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

        public String toString() {
            return "{"+meteodate+":"+fieldname+"=" + value + "}";
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


        //  public String toSQL() {
        //     return "UPDATE public.observ SET " + fieldname + " = "+value+" WHERE id = "+id+";" +
        //     "INSERT INTO public.observ (id, MeteoDate, MeteoTime, "+fieldname+") VALUES ("+id+", "+meteodate+", "+meteotime+", "+value+");";
        // }
        

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
        input.add(new Pair("vishenskoe_water_color","/home/ilya/galina_import/data/Bol_cvet.csv",7));
        input.add(new Pair("vishenskoe_el_conduct","/home/ilya/galina_import/data/Bol_El.csv",7));
        input.add(new Pair("vishenskoeph","/home/ilya/galina_import/data/Bol_pH.csv",7));
        input.add(new Pair("vishenskoe_temp","/home/ilya/galina_import/data/Bol_Temp.csv",7));
        input.add(new Pair("vishenskoe_water_level","/home/ilya/galina_import/data/Bol_Uroven.csv",7));
        input.add(new Pair("kalachik_water_color","/home/ilya/galina_import/data/Kalachik_cvet.csv",18));
        input.add(new Pair("kalachik_el_conduct","/home/ilya/galina_import/data/Kalachik_El.csv",18));
        input.add(new Pair("kalachikph","/home/ilya/galina_import/data/Kalachik_pH.csv",18));
        input.add(new Pair("kalachik_temp","/home/ilya/galina_import/data/Kalachik_Temp.csv",18));
        input.add(new Pair("kalachik_water_level","/home/ilya/galina_import/data/Kalachik_Uroven.csv",18));
        input.add(new Pair("kerzhenets_water_color","/home/ilya/galina_import/data/Kerzhenets_cvet.csv",7));
        input.add(new Pair("kerzhenets_el_conduct","/home/ilya/galina_import/data/Kerzhenets_El.csv",7));
        input.add(new Pair("kerzhenetsph","/home/ilya/galina_import/data/Kerzhenets_pH.csv",7));
        input.add(new Pair("kerzhenets_temp","/home/ilya/galina_import/data/Kerzhenets_Temp.csv",7));
        input.add(new Pair("kerzhenets_water_level","/home/ilya/galina_import/data/Kerzhenets_Uroven.csv",7));
        input.add(new Pair("krugloe_water_color","/home/ilya/galina_import/data/Krugloe_cvet.csv",16));
        input.add(new Pair("krugloe_el_Conduct","/home/ilya/galina_import/data/Krugloe_El.csv",16));
        input.add(new Pair("krugloeph","/home/ilya/galina_import/data/Krugloe_pH.csv",16));
        input.add(new Pair("krugloe_temp","/home/ilya/galina_import/data/Krugloe_Temp.csv",16));
        input.add(new Pair("krugloe_water_level","/home/ilya/galina_import/data/Krugloe_Uroven.csv",16));
        input.add(new Pair("makhovskoe_water_level","/home/ilya/galina_import/data/Makhovskoe_Uroven.csv",18));
        input.add(new Pair("makhovskoe_water_color","/home/ilya/galina_import/data/Makhovskoe_cvet.csv",18));
        input.add(new Pair("makhovskoe_el_conduct","/home/ilya/galina_import/data/Makhovskoe_El.csv",18));
        input.add(new Pair("makhovskoeph","/home/ilya/galina_import/data/Makhovskoe_pH.csv",18));
        input.add(new Pair("makhovskoe_temp","/home/ilya/galina_import/data/Makhovskoe_Temp.csv",18));
        input.add(new Pair("nrustayskoye_water_level","/home/ilya/galina_import/data/NRustayskoye_Uroven.csv",18));
        input.add(new Pair("nrustayskoye_water_color","/home/ilya/galina_import/data/NRustayskoye_cvet.csv",7));
        input.add(new Pair("nrustayskoye_el_conduct","/home/ilya/galina_import/data/NRustayskoye_El.csv",7));
        input.add(new Pair("nrustayskoyeph", "/home/ilya/galina_import/data/NRustayskoye_pH.csv",7));
        input.add(new Pair("nrustayskoye_temp","/home/ilya/galina_import/data/NRustayskoye_Temp.csv",7));
        input.add(new Pair("vishnya_water_color","/home/ilya/galina_import/data/Vishnya_cvet.csv",7));
        input.add(new Pair("vishnya_el_conduct","/home/ilya/galina_import/data/Vishnya_El.csv",7));
        input.add(new Pair("vishnyaph","/home/ilya/galina_import/data/Vishnya_pH.csv",7));
        input.add(new Pair("vishnya_temp","/home/ilya/galina_import/data/Vishnya_Temp.csv",7));
        input.add(new Pair("vishnya_water_level","/home/ilya/galina_import/data/Vishnya_Uroven.csv",7));
        input.add(new Pair("well_water_color","/home/ilya/galina_import/data/Well_cvet.csv",7));
        input.add(new Pair("well_el_conduct","/home/ilya/galina_import/data/Well_El.csv",7));
        input.add(new Pair("wellph","/home/ilya/galina_import/data/Well_pH.csv",7));
        input.add(new Pair("well_temp","/home/ilya/galina_import/data/Well_Temp.csv",7));
        input.add(new Pair("well_water_level","/home/ilya/galina_import/data/Well_Uroven.csv",7));
    }

    public void run() throws Exception {
        String DB_URL     = "jdbc:postgresql://rc1b-h2ibywz9kbvpjomr.mdb.yandexcloud.net:6432/meteo?targetServerType=master&ssl=true&sslmode=verify-full";
        String DB_USER    = "meteo";
        String DB_PASS    = "meteometeo";
        Class.forName("org.postgresql.Driver");
        

 //       input.parallelStream().forEach(p -> {
    Pair p = input.get(39); 

            try (FileReader fr = new FileReader(p.value);
                 BufferedReader br = new BufferedReader(fr);
                 Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);) {
                String line ="";
                Statement stmt = null;
                while (line!=null) {
                    line = br.readLine();
                    if (line!=null){
                        String[] values=line.split(";");
                        //Arrays.stream(values).forEach(v->System.out.println(v));
                        for (int i=1; i<values.length-1; i++){ //i<Observ.years.length-1
                            if (values[i].length()>0) {
                                try{
                                    Observ o = new Observ(values[0].substring(0,5)+"."+String.format("%02d", p.year+i),
                                    p.key,
                                    values[i]);

                                    System.out.println("UPDATE public.observ SET " + o.fieldname + " = "+o.value+" WHERE id = "+o.id+";");

                                    stmt = conn.createStatement();
                                    stmt.executeUpdate("UPDATE public.observ SET " + o.fieldname + " = "+o.value+" WHERE id = "+o.id+";");
                                    stmt.close();
                                }catch(Exception e){
                                    e.printStackTrace();
                                    System.out.println("values[0]="+values[0]+" i="+i + " values.length="+values.length + " values[i]="+values[i]);
                                    System.exit(0);
                                }
                            }
                        }                        
                    } 
                }
                
            } catch (Exception e){
                e.printStackTrace();
            }


//        });
    };

    public static void main(String[] args) {
        App a = new App();
        a.init();
       // a.create_empty();
       try {
        a.run();
       } catch (Exception e){
        e.printStackTrace();
       }
       
    }

    public void create_empty(){
        String DB_URL     = "jdbc:postgresql://rc1b-h2ibywz9kbvpjomr.mdb.yandexcloud.net:6432/meteo?targetServerType=master&ssl=true&sslmode=verify-full";
        String DB_USER    = "meteo";
        String DB_PASS    = "meteometeo";
        String meteodate;
        LocalDate parsedDate;
        LocalTime parsedTime = LocalTime.parse("00:00", DateTimeFormatter.ofPattern("HH:mm"));
        ResultSet q;
        long id;
        Statement stmt = null;

        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);

            parsedDate = LocalDate.parse("01.01.08", DateTimeFormatter.ofPattern("dd.MM.yy"));
            
            // id = LocalDateTime.of(parsedDate, parsedTime).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            // meteodate = "01.01.08";
            // q = conn.createStatement().executeQuery("INSERT INTO public.observ (id, meteo_date, meteo_time, wind_dir, wind_speed, pressure, temp_min, temp_max, precipitation, sunshine_duration, dew_point, snow_level_weather_site, snow_coverage, snow_level_forest, average_snow_density, moisture_water_in_snow,snow_cover,snow_state,nrustayskoye_water_level,nrustayskoye_temp,nrustayskoyeph,nrustayskoye_el_conduct, nrustayskoye_water_color,nrustayskoye_oxygen,nrustayskoyebpk5,krugloe_water_level,krugloe_temp,krugloeph,krugloe_el_conduct,krugloe_water_color,krugloe_oxygen,krugloebpk5,kalachik_water_level,kalachik_temp,kalachikph,kalachik_el_conduct,kalachik_water_color,kalachik_oxygen,kalachikbpk5,makhovskoe_water_level,makhovskoe_temp,makhovskoeph,makhovskoe_el_conduct,makhovskoe_water_color,makhovskoe_oxygen,makhovskoebpk5,kerzhenets_water_level,kerzhenets_temp,kerzhenetsph,kerzhenets_el_conduct,kerzhenets_water_color,kerzhenets_oxygen,kerzhenetsbpk5,vishnya_water_level,vishnya_temp,vishnyaph,vishnya_el_conduct,vishnya_water_color,vishnya_oxygen,vishnyabpk5,well_water_level,well_temp,wellph,well_el_conduct,well_water_color,well_oxygen,wellbpk5,vishenskoe_water_level,vishenskoe_temp,vishenskoeph,vishenskoe_el_conduct,vishenskoe_water_color,vishenskoe_oxygen,vishenskoebpk5) VALUES ("+id+",'" + meteodate + "','00:00','---',0.0,0,0.0,0.0,0.0,0.0,0.0,0,0,0,0.0,0,'---','---',0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0)");
            // if(q.next()) {System.out.println(q.getString(1));}

            while (parsedDate.getYear()<2023) {
                meteodate = parsedDate.format(DateTimeFormatter.ofPattern("dd.MM.yy"));
                id = LocalDateTime.of(parsedDate, parsedTime).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

                stmt = conn.createStatement();
                stmt.executeUpdate("INSERT INTO public.observ (id, meteo_date, meteo_time, wind_dir, wind_speed, pressure, temp_min, temp_max, precipitation, sunshine_duration, dew_point, snow_level_weather_site, snow_coverage, snow_level_forest, average_snow_density, moisture_water_in_snow,snow_cover,snow_state,nrustayskoye_water_level,nrustayskoye_temp,nrustayskoyeph,nrustayskoye_el_conduct, nrustayskoye_water_color,nrustayskoye_oxygen,nrustayskoyebpk5,krugloe_water_level,krugloe_temp,krugloeph,krugloe_el_conduct,krugloe_water_color,krugloe_oxygen,krugloebpk5,kalachik_water_level,kalachik_temp,kalachikph,kalachik_el_conduct,kalachik_water_color,kalachik_oxygen,kalachikbpk5,makhovskoe_water_level,makhovskoe_temp,makhovskoeph,makhovskoe_el_conduct,makhovskoe_water_color,makhovskoe_oxygen,makhovskoebpk5,kerzhenets_water_level,kerzhenets_temp,kerzhenetsph,kerzhenets_el_conduct,kerzhenets_water_color,kerzhenets_oxygen,kerzhenetsbpk5,vishnya_water_level,vishnya_temp,vishnyaph,vishnya_el_conduct,vishnya_water_color,vishnya_oxygen,vishnyabpk5,well_water_level,well_temp,wellph,well_el_conduct,well_water_color,well_oxygen,wellbpk5,vishenskoe_water_level,vishenskoe_temp,vishenskoeph,vishenskoe_el_conduct,vishenskoe_water_color,vishenskoe_oxygen,vishenskoebpk5) VALUES ("+id+",'" + meteodate + "','00:00','---',0.0,0,0.0,0.0,0.0,0.0,0.0,0,0,0,0.0,0,'---','---',0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0,0,0.0,0.0,0,0,0.0,0.0);");
                stmt.close();

                //if(q.next()) {System.out.println("meteodate: " + q.getString(1));}
               System.out.println("meteodate: "+meteodate);
                parsedDate = parsedDate.plusDays(1);
            }

            conn.close();
        } catch(Exception ex) {
                ex.printStackTrace();
        }
    }
}
        // DELETE FROM public.observ WHERE id=1199145600000;
        // select * from public.observ