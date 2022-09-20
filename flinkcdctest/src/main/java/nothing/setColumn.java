package nothing;

import java.util.HashSet;

public class setColumn {
    public static void main(String[] args) {
        HashSet<String> strings = new HashSet<>();
        String[] ss = new String[]{
                "ADR6", "AFKO", "AFPO", "AFRU", "AFVC", "AFVV", "AUFK", "AUFM", "BNKA", "BUT000", "CDHDR", "CDPOS", "CRHD",
                "DUMMY", "EINA", "EINE", "eket", "ekpo", "EKPO", "JEST", "KNA1", "KNB1", "KNBK", "KNVI", "KNVV", "LFA1", "LFM1",
                "LIKP", "LIPS", "MAKT", "MARA", "MARC", "MARD", "MAST", "MATDOC", "MBEW", "MSEG", "NSDM", "NSDM_V_MARC",
                "NSDM_V_MSEG", "NSDM_V_MSKA", "prcd_elements", "QALS", "QAMB", "RESB", "STPO", "T001L", "T003P",
                "T005H", "T005T", "T005U", "T023T", "t024", "t024d", "t024e", "T025T", "T052U", "t156t", "t161t", "t171t",
                "TBRCT", "TCURR", "TJ02T", "TKUKT", "tlogt", "TVGRT", "TVK1T", "UKMBP_CMS", "VBAK", "VBAP", "VBEP", "VBKD",
                "VBPA", "ZFCT0006", "ZFICO_009_TBL", "ZFICO_010_TBL", "ZMM009_TBL", "ZMM014_PURCHASE1",
                "ZMM014_PURCHASE2", "ZMMT0014", "ZPP004_TBL", "ZPP019_PRODUCE", "ZPP020_ASSEMBLY",
                "ZPP021_STOCK", "ZSD_015_CUSTOMER", "ZSD_015_MANAGER", "ZSD_015_PRODTYPE", "ZSD019_DELIVERY",
                "ZTDJXH", "ZTWLCWB", "ZTXTGG", "ZTXTXH", "ZTYQBZZB", "ZTYQYQZB", "ZTYSZB", "ZWLQTCS"};

        for (String s : ss) {
            strings.add(s);
        }

        for (String string : strings) {
            System.out.println(string);
        }

    }
}
