package com.dima_z.indexes.testCases;

import java.util.HashMap;

public class TestCasesFactory {
    
    public static HashMap<String, String[]> getTestCases(QueryType type) {
        if (type == QueryType.ENDS_WITH) {
            return endsWithQueries;
        } else if (type == QueryType.INCLUDES) {
            return includesQueries;
        } else if (type == QueryType.SEARCH) {
            return searchQueries;
        } else if (type == QueryType.STARTS_WITH) {
            return startsWithQueries;
        } else if (type == QueryType.NOT_ENDS_WITH) {
            return endsWithQueries;
        } else if (type == QueryType.NOT_INCLUDES) {
            return includesQueries;
        } else if (type == QueryType.NOT_STARTS_WITH) {
            return startsWithQueries;
        }
        return null;
    }

    private static HashMap<String, String[]> includesQueries = new HashMap<String, String[]>(){
        {
            put("big_result/small_input", new String[]{"de", " de ", "-ye", "Va", "a C", "st", "Gr", "Roc", "am", "ill", "Park"});
            put("big_result/big_input", new String[]{" de la M", "Public", "Plaza", "Argentina", "anada de", "ocky ", "otel ", "Hotel ", "entral ", "South"});
            put("small_result/small_input", new String[]{"ix", " Roc ", "mn", "n v", "omo ", "mln", "klk", "Wk", "zz", "kz", "pz", "knk", "klk"});
            put("small_result/big_input", new String[]{"wne Pla", " Yaw", "shkhara", "angounel", "y de les ", "sera de la ", "rra de la Cr", "epentance Creek Public Schoo", "rranc de la Solana del Pas de la Ca", "nsmitter ( 1170 kHz); Tow", "zat e reja ;New Cemetery ; of Xhafell"});
            put("empty_result", new String[]{"zat e reja ;New Cemetery ; of Xhafelln", "ppirn", "wnp", "thabbaya Mediumw", "Amuntr de", "del o", "anakp", "anakl", "amapz", "amanln"});
        }
    };

    private static HashMap<String, String[]> startsWithQueries = new HashMap<String, String[]>(){
        {
            put("big_result", new String[]{"T", "An", "Ca", "Da", "De", "Na", "Mas", "Ka", "Mc", "W", "Ro", "Sa"});
            put("small_result", new String[]{"Dc", "pr", "Mn", "na", "ka", "Sant ", "Mira ", "Wa ", "W ", "St R", "Sant Miquel"});
            put("empty_result", new String[]{"Ca ", "kp", "proa", " ", "Aa ", "vb", "Sant Miquel de  ", "Sant Miel", "lap", "wad"});
        }
    };

    private static HashMap<String, String[]> endsWithQueries = new HashMap<String, String[]>(){
        {
            put("big_result", new String[]{"an", "a", "se", "o", "ro", "mo", "y", "es", "as", "c"});
            put("small_result", new String[]{"ista", "ny", "xy", "aq", "ayw", "oc", "ralm", "ral", "neralm", "er", "aws"});
            put("empty_result", new String[]{"poc", "qneralm", "peer", "pwer", "wsee", "vews", "qews", "xews", "zews", "asd"});
        }
    };

    private static HashMap<String, String[]> searchQueries = new HashMap<String, String[]>(){
        {
            put("small_input", new String[]{"Travenc", "Tolse", "La Trava", "Artenu", "Prats", "Llorts", "Coll Pa", "Lloset", "Aygal", "Wafd", "Tut", "Padah", "Kim"});
            put("big_input", new String[]{"Cami del Pla de les Pedres", "Estany dels Meners de la Coma", "Cresta del Forat dels Malhiverns", "Kharimat Umm al Muwayghir", "Madrasat Muhammad Bin Khalid ath Thanawiyah", "Markaz ash Shaykh Zayid lith Thaqafah al Bakistaniyah", "Forat del Riu dels Clots de Massat", "Shaykh Khalid Bin Sultan al Qassimi Square", "Le Royal Meridien Beach Resort Spa Dubai", "Qaryah-ye Haji Muhammad Afzal Khan", "Darya-ye Khushk-e Khinjak Manah"});
            put("empty_result", new String[]{"olse", "Cami del Pla", "Trava", "arimat Umm al Muwa", "Tu", "Muhammad Bin Khalid", "Forat del Riu", "Shaykh Zayid lith", "Shaykh Khalid Bin Sultan al Qassimi Squar", "Coll ", "Madrasat Muhammad Bin Khalid ath Thanawiya"});
        }
    };
}
