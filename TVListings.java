/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tvlistings;

import java.util.ArrayList;
import java.io.*;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Admin
 */
public class TVListings {

    /**
     * @param args the command line arguments
     */
    static int pId = 1000;

    static int uId = 100;
    
    static int rId = 1000;

    static String pName = "Prg";

    static String uName = "User";

    static ArrayList programList;

    static String[] categoryList = {"GEC",
        "Movies",
        "News",
        "Infotainment",
        "Kids",
        "LifeStyle",
        "Music",
        "Religious",
        "Sports"};

    static String[] gecChannels = {"Star_World",
        "Comedy_Central",
        "AXN",
        "Fox_Crime",
        "Sony",
        "E_Tv",
        "Star_Plus",
        "Colors"
    };

    static String[] movChannels = {"Star_Movies",
        "Movies_Now",
        "HBO",
        "Sony_pix",
        "Sony_Max",
        "Z_Cinema",
        "Maa_Movies",
        "Gemini_Movies",
        "UTV_Movies"};

    static String[] newsChannels = {"Times_Now",
        "CNN_IBN",
        "BBC_NEWS",
        "Headlines_Today",
        "TV9",
        "N_TV",
        "India_TV",
        "News_24",
        "TV5"};

    static String[] infoChannels = {"Home_Shop_18",
        "Telebrands",
        "National_Geographic_Channel",
        "Discovery_Channel",
        "Animal_planet",
        "NDTV_Good_Times",
        "TLC",
        "Care_World",
        "Topper"};

    static String[] kidsChannels = {"nikoledian",
        "pogo",
        "cartoon_network",
        "discovery_kids",
        "hungama"};

    static String[] lsChannels = {"Z_Khaana",
        "Turbo",
        "Travel_XP",
        "TLC",
        "Good_Times"};

    static String[] musicChannels = {"M_TV",
        "Gemini_Music",
        "B4U",
        "zoom",
        "Zetc"};

    static String[] rlgChannels = {"bhakthi",
        "sanskaar",
        "subhsandesh",
        "TTD",
        "Z_salaam",
        "SVBC",
        "OM"};

    static String[] sprtChannels = {"Sony_six",
        "Star_sports",
        "Ten_sports",
        "Neo_sports",
        "DD_sports",
        "Star_HD",
        "Star_sports2"};

    static String[] gecGenre = {"Action",
        "Action",
        "Game_show",
        "Comedy",
        "Reality_show", 
        "drama",
        "talk_show",
        "travel",
        "Detective"};

    static String[] movGenre = {"Film/Action",
        "Film/Drama",
        "Film/crime",
        "Film/Horror",
        "Film/Adventure",
        "Film/Sci-Fi",
        "Film/Animation",
        "Film/Thriller",
        "Film/Comedy"};

    static String[] newsGenre = {"Interviews",
        "talk_show",
        "Market_watch",
        "Special_report",
        "Headlines",
        "health_and_well_being",
        "magazine",
        "chat_show",
        "Technology",
        "Interviews"};

    static String[] infoGenre = {"Cooking",
        "Social",
        "Astrology",
        "Nature",
        "Space",
        "Technology",
        "Factual_entertainment",
        "Education",
        "Consumer",
        "Biography"
    };

    static String[] kidsGenre = {"Animation",
        "Music",
        "Entertainment",
        "Education",
        "Film/Telefilm"};

    static String[] lsGenre = {"health_and_well_being",
        "Cooking",
        "travel",
        "fashion",
        "culture"};

    static String[] musicGenre = {"Request",
        "drama",
        "Filmi",
        "Consumer",
        "Gameshow",
        "Telelost"};

    static String[] rlgGenre = {"Events/festival",
        "Magazine_programme",
        "Astrology",
        "Entertainment",
        "Award_show",
        "chat_show"};

    static String[] sportsGenre = {"Football",
        "martial_Arts",
        "cricket",
        "Racing",
        "Golf",
        "special_feature",
        "hockey",
        "Tennis",
        "badminton"};
    
    static String[] programViewTime = {"Morning", "Afternoon", "Evening", "Night"};

    static String[] userGender = {"male", "Female"};

    static String[] userTiming = {"Morning", "Afternoon", "Evening", "Night"};

    static int getBinaryRandom(int max) {
        //double value = new Random().nextInt(1);
        double value = Math.random();
        //System.out.println(value);

        if (value > 0.5) {
            return 1;
        } else {
            return 0;
        }

    }

    public static void main(String[] args) {

        int pRecords;
        pRecords = 500;
        int uRecords = 1000;

        int ctIndex, chIndex, gIndex, wIndex, yIndex, wkIndex, uAge, uTime, pIndex;

        StringBuffer pSB = new StringBuffer();
        StringBuffer uSB = new StringBuffer();

        programList = new ArrayList();

        String uHeaders = "Record_Id,U_Name,U_Id,U_Age,U_Gender,U_IsWorking,U_Time,U_Weekday,U_Weekend";

        String pHeaders = "P_Name,P_Id,P_Category,P_Channel,P_Genre,P_SubGenre,P_IsCastFamous,P_Weekend,P_Weekday,P_3MonthSeries,P_3YearSeries,P_Time,P_Rating";

        pSB.append(uHeaders).append(',').append(pHeaders);

        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("C:\\Users\\Admin\\Documents\\iiit\\SemProject\\TVr\\logs\\program.csv", true)))) {
            // TODO code application logic here

            out.println(pSB);

            pSB.setLength(0);

            //pSB.append(pHeaders);            
            for (int i = 0; i < pRecords; i++) {

                pSB.setLength(0);

                pId++;

                pSB.append(pName).append(Integer.toString(pId)).append(',');
                pSB.append(Integer.toString(pId)).append(',');
                ctIndex = new Random().nextInt(categoryList.length);
                pSB.append(categoryList[ctIndex]).append(',');
                switch (categoryList[ctIndex]) {

                    case "GEC": {
                        chIndex = new Random().nextInt(gecChannels.length);
                        pSB.append(gecChannels[chIndex]).append(',');
                        gIndex = new Random().nextInt(gecGenre.length);
                        pSB.append(gecGenre[gIndex]).append(',');

                    }
                    break;

                    case "Movies": {
                        chIndex = new Random().nextInt(movChannels.length);
                        pSB.append(movChannels[chIndex]).append(',');
                        gIndex = new Random().nextInt(movGenre.length);
                        pSB.append(movGenre[gIndex]).append(',');
                    }
                    break;

                    case "News": {
                        chIndex = new Random().nextInt(newsChannels.length);
                        pSB.append(newsChannels[chIndex]).append(',');
                        gIndex = new Random().nextInt(newsGenre.length);
                        pSB.append(newsGenre[gIndex]).append(',');
                    }

                    break;

                    case "Infotainment": {
                        chIndex = new Random().nextInt(infoChannels.length);
                        pSB.append(infoChannels[chIndex]).append(',');
                        gIndex = new Random().nextInt(infoGenre.length);
                        pSB.append(infoGenre[gIndex]).append(',');
                    }

                    break;

                    case "Kids": {
                        chIndex = new Random().nextInt(kidsChannels.length);
                        pSB.append(kidsChannels[chIndex]).append(',');
                        gIndex = new Random().nextInt(kidsGenre.length);
                        pSB.append(kidsGenre[gIndex]).append(',');
                    }
                    break;

                    case "LifeStyle": {
                        chIndex = new Random().nextInt(lsChannels.length);
                        pSB.append(lsChannels[chIndex]).append(',');
                        gIndex = new Random().nextInt(lsGenre.length);
                        pSB.append(lsGenre[gIndex]).append(',');
                    }

                    break;

                    case "Music": {
                        chIndex = new Random().nextInt(musicChannels.length);
                        pSB.append(musicChannels[chIndex]).append(',');
                        gIndex = new Random().nextInt(musicGenre.length);
                        pSB.append(musicGenre[gIndex]).append(',');
                    }
                    break;

                    case "Sports": {
                        chIndex = new Random().nextInt(sprtChannels.length);
                        pSB.append(sprtChannels[chIndex]).append(',');
                        gIndex = new Random().nextInt(sportsGenre.length);
                        pSB.append(sportsGenre[gIndex]).append(',');
                    }
                    break;

                    case "Religious": {
                        chIndex = new Random().nextInt(rlgChannels.length);
                        pSB.append(rlgChannels[chIndex]).append(',');
                        gIndex = new Random().nextInt(rlgGenre.length);
                        pSB.append(rlgGenre[gIndex]).append(',');
                    }
                    break;

                }

                //Sub-Genre
                pSB.append('-').append(',');

                //Cast
                pSB.append(Integer.toString(getBinaryRandom(1))).append(',');

                //WeekDays
                wIndex = getBinaryRandom(1);
                pSB.append(Integer.toString(wIndex)).append(',');

                //WeekEnds
                if (wIndex == 1) {
                    pSB.append(Integer.toString(getBinaryRandom(1))).append(',');
                } else {
                    pSB.append('1').append(',');
                }

                yIndex = getBinaryRandom(1);

                if (yIndex == 1) {
                    wkIndex = 1;
                } else {
                    wkIndex = getBinaryRandom(1);
                }

                pSB.append(wkIndex).append(',');

                pSB.append(yIndex).append(',');
                
                pIndex = new Random().nextInt(programViewTime.length);
                
                pSB.append(programViewTime[pIndex]).append(',');
                
                //pSB.append(Integer.toString((new Random().nextInt(5))+1));

                System.out.println(pSB);

                StringBuffer temp = new StringBuffer();
                temp.append(pSB);
                programList.add(temp);

            }

            for (int u = 0; u < uRecords; u++) {

                uSB.setLength(0);

                uId++;

                uSB.append(uName).append(Integer.toString(uId)).append(',');
                uSB.append(Integer.toString(uId)).append(',');

                uAge = (new Random().nextInt(60) + 5);
                uSB.append(Integer.toString(uAge)).append(',');

                uSB.append(userGender[getBinaryRandom(1)]).append(',');

                if (uAge > 18) {
                    uSB.append(getBinaryRandom(1)).append(',');
                } else {
                    uSB.append('0').append(',');
                }

                uTime = new Random().nextInt(userTiming.length);

                uSB.append(userTiming[uTime]).append(',');

                //WeekDays
                wIndex = getBinaryRandom(1);
                uSB.append(Integer.toString(wIndex)).append(',');

                //WeekEnds
                if (wIndex == 1) {
                    uSB.append(Integer.toString(getBinaryRandom(1))).append(',');
                } else {
                    uSB.append('1').append(',');
                }

                for (int i = 0; i < 15; i++) {
                    
                    rId++;
                    out.print(Integer.toString(rId)+',');                    
                    
                    out.print(uSB);

                    out.print(((StringBuffer) programList.get((new Random().nextInt(programList.size() - 1)) + 1)));
                    
                    out.print(Integer.toString((new Random().nextInt(5))+1));
                    //out.print(pSB);
                    out.println();

                }

            }

        } catch (IOException ex) {
            Logger.getLogger(TVListings.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
