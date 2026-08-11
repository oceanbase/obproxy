/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

static const char german2[] =
    "&AE << \\u00E6 <<< \\u00C6 << \\u00E4 <<< \\u00C4 "
    "&OE << \\u0153 <<< \\u0152 << \\u00F6 <<< \\u00D6 "
    "&UE << \\u00FC <<< \\u00DC ";
static const char icelandic[] =
    "& A < \\u00E1 <<< \\u00C1 "
    "& D < \\u00F0 <<< \\u00D0 "
    "& E < \\u00E9 <<< \\u00C9 "
    "& I < \\u00ED <<< \\u00CD "
    "& O < \\u00F3 <<< \\u00D3 "
    "& U < \\u00FA <<< \\u00DA "
    "& Y < \\u00FD <<< \\u00DD "
    "& Z < \\u00FE <<< \\u00DE "
    "< \\u00E6 <<< \\u00C6 << \\u00E4 <<< \\u00C4 "
    "< \\u00F6 <<< \\u00D6 << \\u00F8 <<< \\u00D8 "
    "< \\u00E5 <<< \\u00C5 ";
static const char latvian[] =
    "& C < \\u010D <<< \\u010C "
    "& G < \\u0123 <<< \\u0122 "
    "& I < \\u0079 <<< \\u0059 "
    "& K < \\u0137 <<< \\u0136 "
    "& L < \\u013C <<< \\u013B "
    "& N < \\u0146 <<< \\u0145 "
    "& R < \\u0157 <<< \\u0156 "
    "& S < \\u0161 <<< \\u0160 "
    "& Z < \\u017E <<< \\u017D ";
static const char romanian[] =
    "& A < \\u0103 <<< \\u0102 < \\u00E2 <<< \\u00C2 "
    "& I < \\u00EE <<< \\u00CE "
    "& S < \\u0219 <<< \\u0218 << \\u015F <<< \\u015E "
    "& T < \\u021B <<< \\u021A << \\u0163 <<< \\u0162 ";
static const char slovenian[] =
    "& C < \\u010D <<< \\u010C "
    "& S < \\u0161 <<< \\u0160 "
    "& Z < \\u017E <<< \\u017D ";
static const char polish[] =
    "& A < \\u0105 <<< \\u0104 "
    "& C < \\u0107 <<< \\u0106 "
    "& E < \\u0119 <<< \\u0118 "
    "& L < \\u0142 <<< \\u0141 "
    "& N < \\u0144 <<< \\u0143 "
    "& O < \\u00F3 <<< \\u00D3 "
    "& S < \\u015B <<< \\u015A "
    "& Z < \\u017A <<< \\u0179 < \\u017C <<< \\u017B";
static const char estonian[] =
    "& S < \\u0161 <<< \\u0160 "
    " < \\u007A <<< \\u005A "
    " < \\u017E <<< \\u017D "
    "& W < \\u00F5 <<< \\u00D5 "
    "< \\u00E4 <<< \\u00C4 "
    "< \\u00F6 <<< \\u00D6 "
    "< \\u00FC <<< \\u00DC ";
static const char spanish[] = "& N < \\u00F1 <<< \\u00D1 ";
static const char swedish[] =
    "& Y <<\\u00FC <<< \\u00DC "
    "& Z < \\u00E5 <<< \\u00C5 "
    "< \\u00E4 <<< \\u00C4 << \\u00E6 <<< \\u00C6 "
    "< \\u00F6 <<< \\u00D6 << \\u00F8 <<< \\u00D8 ";
static const char turkish[] =
    "& C < \\u00E7 <<< \\u00C7 "
    "& G < \\u011F <<< \\u011E "
    "& H < \\u0131 <<< \\u0049 "
    "& O < \\u00F6 <<< \\u00D6 "
    "& S < \\u015F <<< \\u015E "
    "& U < \\u00FC <<< \\u00DC ";
static const char czech[] =
    "& C < \\u010D <<< \\u010C "
    "& H <      ch <<<      Ch <<< CH"
    "& R < \\u0159 <<< \\u0158"
    "& S < \\u0161 <<< \\u0160"
    "& Z < \\u017E <<< \\u017D";
static const char danish[] =
    "& Y << \\u00FC <<< \\u00DC << \\u0171 <<< \\u0170"
    "& Z  < \\u00E6 <<< \\u00C6 << \\u00E4 <<< \\u00C4"
    " < \\u00F8 <<< \\u00D8 << \\u00F6 <<< \\u00D6 << \\u0151 <<< \\u0150"
    " < \\u00E5 <<< \\u00C5 << aa <<<  Aa <<< AA";
static const char lithuanian[] =
    "& C << ch <<< Ch <<< CH< \\u010D <<< \\u010C"
    "& E << \\u0119 <<< \\u0118 << \\u0117 <<< \\u0116"
    "& I << y <<< Y"
    "& S  < \\u0161 <<< \\u0160"
    "& Z  < \\u017E <<< \\u017D";
static const char slovak[] =
    "& A < \\u00E4 <<< \\u00C4"
    "& C < \\u010D <<< \\u010C"
    "& H < ch <<< Ch <<< CH"
    "& O < \\u00F4 <<< \\u00D4"
    "& S < \\u0161 <<< \\u0160"
    "& Z < \\u017E <<< \\u017D";
static const char spanish2[] =
    "&C <  ch <<< Ch <<< CH"
    "&L <  ll <<< Ll <<< LL"
    "&N < \\u00F1 <<< \\u00D1";
static const char roman[] =
    "& I << j <<< J "
    "& V << u <<< U ";
static const char persian[] =
    "& \\u066D < \\u064E < \\uFE76 < \\uFE77 < \\u0650 < \\uFE7A < \\uFE7B"
    " < \\u064F < \\uFE78 < \\uFE79 < \\u064B < \\uFE70 < \\uFE71"
    " < \\u064D < \\uFE74 < \\u064C < \\uFE72"
    "& \\uFE7F < \\u0653 < \\u0654 < \\u0655 < \\u0670"
    "& \\u0669 < \\u0622 < \\u0627 < \\u0671 < \\u0621 < \\u0623 < \\u0625"
    " < \\u0624 < \\u0626"
    "& \\u0642 < \\u06A9 < \\u0643"
    "& \\u0648 < \\u0647 < \\u0629 < \\u06C0 < \\u06CC < \\u0649 < \\u064A"
    "& \\uFE80 < \\uFE81 < \\uFE82 < \\uFE8D < \\uFE8E < \\uFB50 < \\uFB51"
    " < \\uFE80 "
        " & \\uFE80 < \\uFE83 < \\uFE84 < \\uFE87 < \\uFE88 < \\uFE85"
    " < \\uFE86 < \\u0689 < \\u068A"
    "& \\uFEAE < \\uFDFC"
    "& \\uFED8 < \\uFB8E < \\uFB8F < \\uFB90 < \\uFB91 < \\uFED9 < \\uFEDA"
    " < \\uFEDB < \\uFEDC"
    "& \\uFEEE < \\uFEE9 < \\uFEEA < \\uFEEB < \\uFEEC < \\uFE93 < \\uFE94"
    " < \\uFBA4 < \\uFBA5 < \\uFBFC < \\uFBFD < \\uFBFE < \\uFBFF"
    " < \\uFEEF < \\uFEF0 < \\uFEF1 < \\uFEF2 < \\uFEF3 < \\uFEF4"
    " < \\uFEF5 < \\uFEF6 < \\uFEF7 < \\uFEF8 < \\uFEF9 < \\uFEFA"
    " < \\uFEFB < \\uFEFC";
static const char esperanto[] =
    "& C < \\u0109 <<< \\u0108"
    "& G < \\u011D <<< \\u011C"
    "& H < \\u0125 <<< \\u0124"
    "& J < \\u0135 <<< \\u0134"
    "& S < \\u015d <<< \\u015c"
    "& U < \\u016d <<< \\u016c";
static const char hungarian[] =
    "&O < \\u00F6 <<< \\u00D6 << \\u0151 <<< \\u0150"
    "&U < \\u00FC <<< \\u00DC << \\u0171 <<< \\u0170";
static const char croatian[] =
    "&C < \\u010D <<< \\u010C < \\u0107 <<< \\u0106"
    "&D < d\\u017E = \\u01C6 <<< d\\u017D <<< D\\u017E = \\u01C5 <<< D\\u017D "
    "= \\u01C4"
    "   < \\u0111 <<< \\u0110"
    "&L < lj = \\u01C9  <<< lJ <<< Lj = \\u01C8 <<< LJ = \\u01C7"
    "&N < nj = \\u01CC  <<< nJ <<< Nj = \\u01CB <<< NJ = \\u01CA"
    "&S < \\u0161 <<< \\u0160"
    "&Z < \\u017E <<< \\u017D";
#if 0
static const char sinhala[]=
    "& \\u0D96 < \\u0D82 < \\u0D83"
    "& \\u0DA5 < \\u0DA4"
    "& \\u0DD8 < \\u0DF2 < \\u0DDF < \\u0DF3"
    "& \\u0DDE < \\u0DCA";
#else
static const char sinhala[] =
    "& \\u0D96 < \\u0D82 < \\u0D83 < \\u0D9A < \\u0D9B < \\u0D9C < \\u0D9D"
    "< \\u0D9E < \\u0D9F < \\u0DA0 < \\u0DA1 < \\u0DA2 < \\u0DA3"
    "< \\u0DA5 < \\u0DA4 < \\u0DA6"
    "< \\u0DA7 < \\u0DA8 < \\u0DA9 < \\u0DAA < \\u0DAB < \\u0DAC"
    "< \\u0DAD < \\u0DAE < \\u0DAF < \\u0DB0 < \\u0DB1"
    "< \\u0DB3 < \\u0DB4 < \\u0DB5 < \\u0DB6 < \\u0DB7 < \\u0DB8"
    "< \\u0DB9 < \\u0DBA < \\u0DBB < \\u0DBD < \\u0DC0 < \\u0DC1"
    "< \\u0DC2 < \\u0DC3 < \\u0DC4 < \\u0DC5 < \\u0DC6"
    "< \\u0DCF"
    "< \\u0DD0 < \\u0DD1 < \\u0DD2 < \\u0DD3 < \\u0DD4 < \\u0DD6"
    "< \\u0DD8 < \\u0DF2 < \\u0DDF < \\u0DF3 < \\u0DD9 < \\u0DDA"
    "< \\u0DDB < \\u0DDC < \\u0DDD < \\u0DDE < \\u0DCA";
#endif
static const char vietnamese[] =
    " &A << \\u00E0 <<< \\u00C0"
    " << \\u1EA3 <<< \\u1EA2"
    " << \\u00E3 <<< \\u00C3"
    " << \\u00E1 <<< \\u00C1"
    " << \\u1EA1 <<< \\u1EA0"
    "  < \\u0103 <<< \\u0102"
    " << \\u1EB1 <<< \\u1EB0"
    " << \\u1EB3 <<< \\u1EB2"
    " << \\u1EB5 <<< \\u1EB4"
    " << \\u1EAF <<< \\u1EAE"
    " << \\u1EB7 <<< \\u1EB6"
    "  < \\u00E2 <<< \\u00C2"
    " << \\u1EA7 <<< \\u1EA6"
    " << \\u1EA9 <<< \\u1EA8"
    " << \\u1EAB <<< \\u1EAA"
    " << \\u1EA5 <<< \\u1EA4"
    " << \\u1EAD <<< \\u1EAC"
    " &D  < \\u0111 <<< \\u0110"
    " &E << \\u00E8 <<< \\u00C8"
    " << \\u1EBB <<< \\u1EBA"
    " << \\u1EBD <<< \\u1EBC"
    " << \\u00E9 <<< \\u00C9"
    " << \\u1EB9 <<< \\u1EB8"
    "  < \\u00EA <<< \\u00CA"
    " << \\u1EC1 <<< \\u1EC0"
    " << \\u1EC3 <<< \\u1EC2"
    " << \\u1EC5 <<< \\u1EC4"
    " << \\u1EBF <<< \\u1EBE"
    " << \\u1EC7 <<< \\u1EC6"
    " &I << \\u00EC <<< \\u00CC"
    " << \\u1EC9 <<< \\u1EC8"
    " << \\u0129 <<< \\u0128"
    " << \\u00ED <<< \\u00CD"
    " << \\u1ECB <<< \\u1ECA"
    " &O << \\u00F2 <<< \\u00D2"
    " << \\u1ECF <<< \\u1ECE"
    " << \\u00F5 <<< \\u00D5"
    " << \\u00F3 <<< \\u00D3"
    " << \\u1ECD <<< \\u1ECC"
    "  < \\u00F4 <<< \\u00D4"
    " << \\u1ED3 <<< \\u1ED2"
    " << \\u1ED5 <<< \\u1ED4"
    " << \\u1ED7 <<< \\u1ED6"
    " << \\u1ED1 <<< \\u1ED0"
    " << \\u1ED9 <<< \\u1ED8"
    "  < \\u01A1 <<< \\u01A0"
    " << \\u1EDD <<< \\u1EDC"
    " << \\u1EDF <<< \\u1EDE"
    " << \\u1EE1 <<< \\u1EE0"
    " << \\u1EDB <<< \\u1EDA"
    " << \\u1EE3 <<< \\u1EE2"
    " &U << \\u00F9 <<< \\u00D9"
    " << \\u1EE7 <<< \\u1EE6"
    " << \\u0169 <<< \\u0168"
    " << \\u00FA <<< \\u00DA"
    " << \\u1EE5 <<< \\u1EE4"
    "  < \\u01B0 <<< \\u01AF"
    " << \\u1EEB <<< \\u1EEA"
    " << \\u1EED <<< \\u1EEC"
    " << \\u1EEF <<< \\u1EEE"
    " << \\u1EE9 <<< \\u1EE8"
    " << \\u1EF1 <<< \\u1EF0"
    " &Y << \\u1EF3 <<< \\u1EF2"
    " << \\u1EF7 <<< \\u1EF6"
    " << \\u1EF9 <<< \\u1EF8"
    " << \\u00FD <<< \\u00DD"
    " << \\u1EF5 <<< \\u1EF4";
static const char de_pb_cldr_30[] =
    "&AE << \\u00E4 <<< \\u00C4 "
    "&OE << \\u00F6 <<< \\u00D6 "
    "&UE << \\u00FC <<< \\u00DC ";
static const char is_cldr_30[] =
    "&[before 1]b       <  \\u00E1 <<< \\u00C1 "
    "&          d       << \\u0111 <<< \\u0110 < \\u00F0 <<< \\u00D0 "
    "&[before 1]f       <  \\u00E9 <<< \\u00C9 "
    "&[before 1]j       <  \\u00ED <<< \\u00CD "
    "&[before 1]p       <  \\u00F3 <<< \\u00D3 "
    "&[before 1]v       <  \\u00FA <<< \\u00DA "
    "&[before 1]z       <  \\u00FD <<< \\u00DD "
    "&[before 1]\\u01C0 <  \\u00E6 <<< \\u00C6 << \\u00E4 <<< \\u00C4 "
    "<  \\u00F6 <<< \\u00D6 << \\u00F8 <<< \\u00D8 "
    "<  \\u00E5 <<< \\u00C5";
static const char lv_cldr_30[] =
    "&[before 1]D       <  \\u010D <<< \\u010C "
    "&[before 1]H       <  \\u0123 <<< \\u0122 "
    "&          I       << y       <<< Y "
    "&[before 1]L       <  \\u0137 <<< \\u0136 "
    "&[before 1]M       <  \\u013C <<< \\u013B "
    "&[before 1]O       <  \\u0146 <<< \\u0145 "
    "&[before 1]S       <  \\u0157 <<< \\u0156 "
    "&[before 1]T       <  \\u0161 <<< \\u0160 "
    "&[before 1]\\u01B7 <  \\u017E <<< \\u017D";
static const char ro_cldr_30[] =
    "&A < \\u0103 <<< \\u0102 <   \\u00E2 <<< \\u00C2 "
    "&I < \\u00EE <<< \\u00CE "
    "&S < \\u015F =   \\u0219 <<< \\u015E =   \\u0218 "
    "&T < \\u0163 =   \\u021B <<< \\u0162 =   \\u021A";
static const char sl_cldr_30[] =
    "&C < \\u010D <<< \\u010C < \\u0107 <<< \\u0106 "
    "&D < \\u0111 <<< \\u0110 "
    "&S < \\u0161 <<< \\u0160 "
    "&Z < \\u017E <<< \\u017D";
static const char pl_cldr_30[] =
    "&A < \\u0105 <<< \\u0104 "
    "&C < \\u0107 <<< \\u0106 "
    "&E < \\u0119 <<< \\u0118 "
    "&L < \\u0142 <<< \\u0141 "
    "&N < \\u0144 <<< \\u0143 "
    "&O < \\u00F3 <<< \\u00D3 "
    "&S < \\u015B <<< \\u015A "
    "&Z < \\u017A <<< \\u0179 < \\u017C <<< \\u017B";
static const char et_cldr_30[] =
    "&[before 1]T <   \\u0161 <<< \\u0160 < z         <<< Z "
    "<   \\u017E <<< \\u017D "
    "&[before 1]X <   \\u00F5 <<< \\u00D5 <   \\u00E4 <<< \\u00C4 "
    "<   \\u00F6 <<< \\u00D6 <   \\u00FC <<< \\u00DC";
static const char sv_cldr_30[] =
    "&          D       <<  \\u0111   <<< \\u0110 <<  \\u00F0 <<< \\u00D0 "
    "&          t       <<< \\u00FE/h "
    "&          T       <<< \\u00DE/H "
    "&          Y       <<  \\u00FC   <<< \\u00DC <<  \\u0171 <<< \\u0170 "
    "&[before 1]\\u01C0 <   \\u00E5   <<< \\u00C5 <   \\u00E4 <<< \\u00C4 "
    "<< \\u00E6   <<< \\u00C6 <<  \\u0119 <<< \\u0118 "
    "<  \\u00F6   <<< \\u00D6 <<  \\u00F8 <<< \\u00D8 "
    "<< \\u0151   <<< \\u0150 <<  \\u0153 <<< \\u0152 "
    "<< \\u00F4   <<< \\u00D4";
static const char tr_cldr_30[] =
    "&          C <   \\u00E7 <<< \\u00C7 "
    "&          G <   \\u011F <<< \\u011E "
    "&[before 1]i <   \\u0131 <<< I "
    "&          i <<< \\u0130 "
    "&          O <   \\u00F6 <<< \\u00D6 "
    "&          S <   \\u015F <<< \\u015E "
    "&          U <   \\u00FC <<< \\u00DC ";
static const char cs_cldr_30[] =
    "&C < \\u010D <<< \\u010C "
    "&H < ch      <<< cH       <<< Ch <<< CH "
    "&R < \\u0159 <<< \\u0158"
    "&S < \\u0161 <<< \\u0160"
    "&Z < \\u017E <<< \\u017D";
static const char da_cldr_30[] =
    "&          D       <<  \\u0111   <<< \\u0110 <<  \\u00F0 <<< \\u00D0 "
    "&          t       <<< \\u00FE/h "
    "&          T       <<< \\u00DE/H "
    "&          Y       <<  \\u00FC   <<< \\u00DC <<  \\u0171 <<< \\u0170 "
    "&[before 1]\\u01C0 <   \\u00E6   <<< \\u00C6 <<  \\u00E4 <<< \\u00C4 "
    "<   \\u00F8   <<< \\u00D8 <<  \\u00F6 <<< \\u00D6 "
    "<<  \\u0151   <<< \\u0150 <<  \\u0153 <<< \\u0152 "
    "<   \\u00E5   <<< \\u00C5 <<< aa      <<< Aa "
    "<<< AA";
// static Coll_param da_coll_param = {nullptr, false, CASE_FIRST_UPPER};
static const char lt_cldr_30[] =
    "&\\u0300 = \\u0307\\u0300 "
    "&\\u0301 = \\u0307\\u0301 "
    "&\\u0303 = \\u0307\\u0303 "
    "&A << \\u0105 <<< \\u0104 "
    "&C <  \\u010D <<< \\u010C "
    "&E << \\u0119 <<< \\u0118 << \\u0117 <<< \\u0116"
    "&I << \\u012F <<< \\u012E << y       <<< Y "
    "&S <  \\u0161 <<< \\u0160 "
    "&U << \\u0173 <<< \\u0172 << \\u016B <<< \\u016A "
    "&Z <  \\u017E <<< \\u017D";
static const char sk_cldr_30[] =
    "&A < \\u00E4 <<< \\u00C4 "
    "&C < \\u010D <<< \\u010C "
    "&H < ch      <<< cH      <<< Ch <<< CH "
    "&O < \\u00F4 <<< \\u00D4 "
    "&R < \\u0159 <<< \\u0158 "
    "&S < \\u0161 <<< \\u0160 "
    "&Z < \\u017E <<< \\u017D";
static const char es_trad_cldr_30[] =
    "&N <  \\u00F1 <<< \\u00D1 "
    "&C <  ch      <<< Ch      <<< CH "
    "&l <  ll      <<< Ll      <<< LL";
#if 0
static const char fa_cldr_30[]=
  "&          \\u064E << \\u0650 << \\u064F <<  \\u064B << \\u064D "
                     "<< \\u064C "
  "&[before 1]\\u0627 <  \\u0622 "
  "&          \\u0627 << \\u0671 <  \\u0621 <<  \\u0623 << \\u0672 "
                     "<< \\u0625 << \\u0673 <<  \\u0624 << \\u06CC\\u0654 "
                     "<<< \\u0649\\u0654    <<< \\u0626 "
  "&          \\u06A9 << \\u06AA << \\u06AB <<  \\u0643 << \\u06AC "
                     "<< \\u06AD << \\u06AE "
  "&          \\u06CF <  \\u0647 << \\u06D5 <<  \\u06C1 << \\u0629 "
                     "<< \\u06C3 << \\u06C0 <<  \\u06BE "
  "&          \\u06CC << \\u0649 << \\u06D2 <<  \\u064A << \\u06D0 "
                     "<< \\u06D1 << \\u06CD <<  \\u06CE";
static Reorder_param fa_reorder_param= {
  {CHARGRP_ARAB, CHARGRP_NONE}, {{{0, 0}, {0, 0}}}, 0
};
static Coll_param fa_coll_param= {
  &fa_reorder_param, true
};
#endif
static const char hu_cldr_30[] =
    "&C  <   cs      <<< Cs      <<< CS "
    "&D  <   dz      <<< Dz      <<< DZ "
    "&DZ <   dzs     <<< Dzs     <<< DZS "
    "&G  <   gy      <<< Gy      <<< GY "
    "&L  <   ly      <<< Ly      <<< LY "
    "&N  <   ny      <<< Ny      <<< NY "
    "&S  <   sz      <<< Sz      <<< SZ "
    "&T  <   ty      <<< Ty      <<< TY "
    "&Z  <   zs      <<< Zs      <<< ZS "
    "&O  <   \\u00F6 <<< \\u00D6 <<  \\u0151 <<< \\u0150 "
    "&U  <   \\u00FC <<< \\u00DC <<  \\u0171 <<< \\u0170 "
    "&cs <<< ccs/cs "
    "&Cs <<< Ccs/cs "
    "&CS <<< CCS/CS "
    "&dz <<< ddz/dz "
    "&Dz <<< Ddz/dz "
    "&DZ <<< DDZ/DZ "
    "&dzs<<< ddzs/dzs "
    "&Dzs<<< Ddzs/dzs "
    "&DZS<<< DDZS/DZS "
    "&gy <<< ggy/gy "
    "&Gy <<< Ggy/gy "
    "&GY <<< GGY/GY "
    "&ly <<< lly/ly "
    "&Ly <<< Lly/ly "
    "&LY <<< LLY/LY "
    "&ny <<< nny/ny "
    "&Ny <<< Nny/ny "
    "&NY <<< NNY/NY "
    "&sz <<< ssz/sz "
    "&Sz <<< Ssz/sz "
    "&SZ <<< SSZ/SZ "
    "&ty <<< tty/ty "
    "&Ty <<< Tty/ty "
    "&TY <<< TTY/TY "
    "&zs <<< zzs/zs "
    "&Zs <<< Zzs/zs "
    "&ZS <<< ZZS/ZS";
static const char hr_cldr_30[] =
    "&C <   \\u010D  <<< \\u010C <   \\u0107  <<< \\u0106 "
    "&D <   d\\u017E <<< \\u01C6 <<< D\\u017E <<< \\u01C5 <<< D\\u017D "
    "<<< \\u01C4  <   \\u0111 <<< \\u0110 "
    "&L <   lj       <<< \\u01C9 <<< Lj       <<< \\u01C8 <<< LJ "
    "<<< \\u01C7 "
    "&N <   nj       <<< \\u01CC <<< Nj       <<< \\u01CB <<< NJ "
    "<<< \\u01CA "
    "&S <   \\u0161  <<< \\u0160 "
    "&Z <   \\u017E  <<< \\u017D ";
// static Reorder_param hr_reorder_param = {
//     {CHARGRP_LATIN, CHARGRP_CYRILLIC, CHARGRP_NONE}, {{{0, 0}, {0, 0}}}, 0, 0};
// static Coll_param hr_coll_param = {&hr_reorder_param, false, CASE_FIRST_OFF};
#if 0
static const char si_cldr_30[]=
  "&\\u0D96 < \\u0D82 < \\u0D83 "
  "&\\u0DA5 < \\u0DA4";
#endif
static const char vi_cldr_30[] =
    "&\\u0300 << \\u0309 <<  \\u0303 << \\u0301 <<  \\u0323 "
    "&a       < \\u0103 <<< \\u0102 <  \\u00E2 <<< \\u00C2 "
    "&d       < \\u0111 <<< \\u0110 "
    "&e       < \\u00EA <<< \\u00CA "
    "&o       < \\u00F4 <<< \\u00D4 <  \\u01A1 <<< \\u01A0 "
    "&u       < \\u01B0 <<< \\u01AF";
// static Coll_param vi_coll_param = {nullptr, true, CASE_FIRST_OFF};
// static Reorder_param ja_reorder_param = {
//         {CHARGRP_LATIN, CHARGRP_KANA, CHARGRP_NONE},
//     {{{0, 0}, {0, 0}}},
//     0,
//     0};
// static Coll_param ja_coll_param = {&ja_reorder_param, false ,
//                                    CASE_FIRST_OFF};
// static Reorder_param zh_reorder_param = {
//     {CHARGRP_NONE}, {{{0x1C47, 0x54A3}, {0xBDC4, 0xF620}}}, 1, 0x54A3};
// static Coll_param zh_coll_param = {&zh_reorder_param, false, CASE_FIRST_OFF};
// static Reorder_param zh2_reorder_param = {
//     {CHARGRP_NONE}, {{{0x1C47, 0x54A3}, {0x5C52, 0x94AE}}}, 1, 0x54A3};
// static Coll_param zh2_coll_param = {&zh2_reorder_param, false, CASE_FIRST_OFF};
// static Reorder_param zh3_reorder_param = {
//     {CHARGRP_NONE}, {{{0x1C47, 0x54A3}, {0x1CB0, 0x550C}}}, 1, 0x54A3};
// static Coll_param zh3_coll_param = {&zh3_reorder_param, false, CASE_FIRST_OFF};
// static Reorder_param ru_reorder_param = {
//     {CHARGRP_CYRILLIC, CHARGRP_NONE}, {{{0, 0}, {0, 0}}}, 0, 0};
// static Coll_param ru_coll_param = {&ru_reorder_param, false ,
//                                    CASE_FIRST_OFF};
// static constexpr uint16 nochar[] = {0, 0};
// static unsigned char ctype_utf8[] = {
//     0,
//    32, 32, 32, 32, 32, 32, 32, 32, 32, 40, 40, 40, 40, 40, 32, 32,
//    32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
//    72, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
//   132,132,132,132,132,132,132,132,132,132, 16, 16, 16, 16, 16, 16,
//    16,129,129,129,129,129,129,  1,  1,  1,  1,  1,  1,  1,  1,  1,
//     1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1, 16, 16, 16, 16, 16,
//    16,130,130,130,130,130,130,  2,  2,  2,  2,  2,  2,  2,  2,  2,
//     2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, 16, 16, 16, 16, 32,
//     3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
//     3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
//     3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
//     3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
//     3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
//     3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
//     3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
//     3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  0
// };
// ObUCAInfo ob_uca_v400 = {
//     UCA_V400,
//     0xFFFF,
//     ob_uca_length, ob_uca_weight, false, nullptr,
//     nullptr,

//     0x0009,
//     0xA48C,
//     0x0332,
//     0x20EA,
//     0x0000,
//     0xFE73,
//     0x0000,
//     0xFE73,
//     0x0000,
//     0x0000,
//     0x0009,
//     0x2183,
//     0,
//     0,
//     0
// };
// ObUCAInfo ob_uca_v520 = {
//     UCA_V520,
//     0x10FFFF,
//     ob_uca520_length,
//     ob_uca520_weight,
//     false,
//     nullptr,
//     nullptr,
//     0x0009,
//     0x1342E,
//     0x0332,
//     0x101FD,
//     0x0000,
//     0xFE73,
//     0x0000,
//     0xFE73,
//     0x0000,
//     0x0000,
//     0x0009,
//     0x1D371,
//     0,
//     0,
//     0
// };
static ObUnicaseInfoChar turk00[] = {
    {0x0000, 0x0000, 0x0000}, {0x0001, 0x0001, 0x0001},
    {0x0002, 0x0002, 0x0002}, {0x0003, 0x0003, 0x0003},
    {0x0004, 0x0004, 0x0004}, {0x0005, 0x0005, 0x0005},
    {0x0006, 0x0006, 0x0006}, {0x0007, 0x0007, 0x0007},
    {0x0008, 0x0008, 0x0008}, {0x0009, 0x0009, 0x0009},
    {0x000A, 0x000A, 0x000A}, {0x000B, 0x000B, 0x000B},
    {0x000C, 0x000C, 0x000C}, {0x000D, 0x000D, 0x000D},
    {0x000E, 0x000E, 0x000E}, {0x000F, 0x000F, 0x000F},
    {0x0010, 0x0010, 0x0010}, {0x0011, 0x0011, 0x0011},
    {0x0012, 0x0012, 0x0012}, {0x0013, 0x0013, 0x0013},
    {0x0014, 0x0014, 0x0014}, {0x0015, 0x0015, 0x0015},
    {0x0016, 0x0016, 0x0016}, {0x0017, 0x0017, 0x0017},
    {0x0018, 0x0018, 0x0018}, {0x0019, 0x0019, 0x0019},
    {0x001A, 0x001A, 0x001A}, {0x001B, 0x001B, 0x001B},
    {0x001C, 0x001C, 0x001C}, {0x001D, 0x001D, 0x001D},
    {0x001E, 0x001E, 0x001E}, {0x001F, 0x001F, 0x001F},
    {0x0020, 0x0020, 0x0020}, {0x0021, 0x0021, 0x0021},
    {0x0022, 0x0022, 0x0022}, {0x0023, 0x0023, 0x0023},
    {0x0024, 0x0024, 0x0024}, {0x0025, 0x0025, 0x0025},
    {0x0026, 0x0026, 0x0026}, {0x0027, 0x0027, 0x0027},
    {0x0028, 0x0028, 0x0028}, {0x0029, 0x0029, 0x0029},
    {0x002A, 0x002A, 0x002A}, {0x002B, 0x002B, 0x002B},
    {0x002C, 0x002C, 0x002C}, {0x002D, 0x002D, 0x002D},
    {0x002E, 0x002E, 0x002E}, {0x002F, 0x002F, 0x002F},
    {0x0030, 0x0030, 0x0030}, {0x0031, 0x0031, 0x0031},
    {0x0032, 0x0032, 0x0032}, {0x0033, 0x0033, 0x0033},
    {0x0034, 0x0034, 0x0034}, {0x0035, 0x0035, 0x0035},
    {0x0036, 0x0036, 0x0036}, {0x0037, 0x0037, 0x0037},
    {0x0038, 0x0038, 0x0038}, {0x0039, 0x0039, 0x0039},
    {0x003A, 0x003A, 0x003A}, {0x003B, 0x003B, 0x003B},
    {0x003C, 0x003C, 0x003C}, {0x003D, 0x003D, 0x003D},
    {0x003E, 0x003E, 0x003E}, {0x003F, 0x003F, 0x003F},
    {0x0040, 0x0040, 0x0040}, {0x0041, 0x0061, 0x0041},
    {0x0042, 0x0062, 0x0042}, {0x0043, 0x0063, 0x0043},
    {0x0044, 0x0064, 0x0044}, {0x0045, 0x0065, 0x0045},
    {0x0046, 0x0066, 0x0046}, {0x0047, 0x0067, 0x0047},
    {0x0048, 0x0068, 0x0048}, {0x0049, 0x0131, 0x0049},
    {0x004A, 0x006A, 0x004A}, {0x004B, 0x006B, 0x004B},
    {0x004C, 0x006C, 0x004C}, {0x004D, 0x006D, 0x004D},
    {0x004E, 0x006E, 0x004E}, {0x004F, 0x006F, 0x004F},
    {0x0050, 0x0070, 0x0050}, {0x0051, 0x0071, 0x0051},
    {0x0052, 0x0072, 0x0052}, {0x0053, 0x0073, 0x0053},
    {0x0054, 0x0074, 0x0054}, {0x0055, 0x0075, 0x0055},
    {0x0056, 0x0076, 0x0056}, {0x0057, 0x0077, 0x0057},
    {0x0058, 0x0078, 0x0058}, {0x0059, 0x0079, 0x0059},
    {0x005A, 0x007A, 0x005A}, {0x005B, 0x005B, 0x005B},
    {0x005C, 0x005C, 0x005C}, {0x005D, 0x005D, 0x005D},
    {0x005E, 0x005E, 0x005E}, {0x005F, 0x005F, 0x005F},
    {0x0060, 0x0060, 0x0060}, {0x0041, 0x0061, 0x0041},
    {0x0042, 0x0062, 0x0042}, {0x0043, 0x0063, 0x0043},
    {0x0044, 0x0064, 0x0044}, {0x0045, 0x0065, 0x0045},
    {0x0046, 0x0066, 0x0046}, {0x0047, 0x0067, 0x0047},
    {0x0048, 0x0068, 0x0048}, {0x0130, 0x0069, 0x0049},
    {0x004A, 0x006A, 0x004A}, {0x004B, 0x006B, 0x004B},
    {0x004C, 0x006C, 0x004C}, {0x004D, 0x006D, 0x004D},
    {0x004E, 0x006E, 0x004E}, {0x004F, 0x006F, 0x004F},
    {0x0050, 0x0070, 0x0050}, {0x0051, 0x0071, 0x0051},
    {0x0052, 0x0072, 0x0052}, {0x0053, 0x0073, 0x0053},
    {0x0054, 0x0074, 0x0054}, {0x0055, 0x0075, 0x0055},
    {0x0056, 0x0076, 0x0056}, {0x0057, 0x0077, 0x0057},
    {0x0058, 0x0078, 0x0058}, {0x0059, 0x0079, 0x0059},
    {0x005A, 0x007A, 0x005A}, {0x007B, 0x007B, 0x007B},
    {0x007C, 0x007C, 0x007C}, {0x007D, 0x007D, 0x007D},
    {0x007E, 0x007E, 0x007E}, {0x007F, 0x007F, 0x007F},
    {0x0080, 0x0080, 0x0080}, {0x0081, 0x0081, 0x0081},
    {0x0082, 0x0082, 0x0082}, {0x0083, 0x0083, 0x0083},
    {0x0084, 0x0084, 0x0084}, {0x0085, 0x0085, 0x0085},
    {0x0086, 0x0086, 0x0086}, {0x0087, 0x0087, 0x0087},
    {0x0088, 0x0088, 0x0088}, {0x0089, 0x0089, 0x0089},
    {0x008A, 0x008A, 0x008A}, {0x008B, 0x008B, 0x008B},
    {0x008C, 0x008C, 0x008C}, {0x008D, 0x008D, 0x008D},
    {0x008E, 0x008E, 0x008E}, {0x008F, 0x008F, 0x008F},
    {0x0090, 0x0090, 0x0090}, {0x0091, 0x0091, 0x0091},
    {0x0092, 0x0092, 0x0092}, {0x0093, 0x0093, 0x0093},
    {0x0094, 0x0094, 0x0094}, {0x0095, 0x0095, 0x0095},
    {0x0096, 0x0096, 0x0096}, {0x0097, 0x0097, 0x0097},
    {0x0098, 0x0098, 0x0098}, {0x0099, 0x0099, 0x0099},
    {0x009A, 0x009A, 0x009A}, {0x009B, 0x009B, 0x009B},
    {0x009C, 0x009C, 0x009C}, {0x009D, 0x009D, 0x009D},
    {0x009E, 0x009E, 0x009E}, {0x009F, 0x009F, 0x009F},
    {0x00A0, 0x00A0, 0x00A0}, {0x00A1, 0x00A1, 0x00A1},
    {0x00A2, 0x00A2, 0x00A2}, {0x00A3, 0x00A3, 0x00A3},
    {0x00A4, 0x00A4, 0x00A4}, {0x00A5, 0x00A5, 0x00A5},
    {0x00A6, 0x00A6, 0x00A6}, {0x00A7, 0x00A7, 0x00A7},
    {0x00A8, 0x00A8, 0x00A8}, {0x00A9, 0x00A9, 0x00A9},
    {0x00AA, 0x00AA, 0x00AA}, {0x00AB, 0x00AB, 0x00AB},
    {0x00AC, 0x00AC, 0x00AC}, {0x00AD, 0x00AD, 0x00AD},
    {0x00AE, 0x00AE, 0x00AE}, {0x00AF, 0x00AF, 0x00AF},
    {0x00B0, 0x00B0, 0x00B0}, {0x00B1, 0x00B1, 0x00B1},
    {0x00B2, 0x00B2, 0x00B2}, {0x00B3, 0x00B3, 0x00B3},
    {0x00B4, 0x00B4, 0x00B4}, {0x039C, 0x00B5, 0x039C},
    {0x00B6, 0x00B6, 0x00B6}, {0x00B7, 0x00B7, 0x00B7},
    {0x00B8, 0x00B8, 0x00B8}, {0x00B9, 0x00B9, 0x00B9},
    {0x00BA, 0x00BA, 0x00BA}, {0x00BB, 0x00BB, 0x00BB},
    {0x00BC, 0x00BC, 0x00BC}, {0x00BD, 0x00BD, 0x00BD},
    {0x00BE, 0x00BE, 0x00BE}, {0x00BF, 0x00BF, 0x00BF},
    {0x00C0, 0x00E0, 0x0041}, {0x00C1, 0x00E1, 0x0041},
    {0x00C2, 0x00E2, 0x0041}, {0x00C3, 0x00E3, 0x0041},
    {0x00C4, 0x00E4, 0x0041}, {0x00C5, 0x00E5, 0x0041},
    {0x00C6, 0x00E6, 0x00C6}, {0x00C7, 0x00E7, 0x0043},
    {0x00C8, 0x00E8, 0x0045}, {0x00C9, 0x00E9, 0x0045},
    {0x00CA, 0x00EA, 0x0045}, {0x00CB, 0x00EB, 0x0045},
    {0x00CC, 0x00EC, 0x0049}, {0x00CD, 0x00ED, 0x0049},
    {0x00CE, 0x00EE, 0x0049}, {0x00CF, 0x00EF, 0x0049},
    {0x00D0, 0x00F0, 0x00D0}, {0x00D1, 0x00F1, 0x004E},
    {0x00D2, 0x00F2, 0x004F}, {0x00D3, 0x00F3, 0x004F},
    {0x00D4, 0x00F4, 0x004F}, {0x00D5, 0x00F5, 0x004F},
    {0x00D6, 0x00F6, 0x004F}, {0x00D7, 0x00D7, 0x00D7},
    {0x00D8, 0x00F8, 0x00D8}, {0x00D9, 0x00F9, 0x0055},
    {0x00DA, 0x00FA, 0x0055}, {0x00DB, 0x00FB, 0x0055},
    {0x00DC, 0x00FC, 0x0055}, {0x00DD, 0x00FD, 0x0059},
    {0x00DE, 0x00FE, 0x00DE}, {0x00DF, 0x00DF, 0x00DF},
    {0x00C0, 0x00E0, 0x0041}, {0x00C1, 0x00E1, 0x0041},
    {0x00C2, 0x00E2, 0x0041}, {0x00C3, 0x00E3, 0x0041},
    {0x00C4, 0x00E4, 0x0041}, {0x00C5, 0x00E5, 0x0041},
    {0x00C6, 0x00E6, 0x00C6}, {0x00C7, 0x00E7, 0x0043},
    {0x00C8, 0x00E8, 0x0045}, {0x00C9, 0x00E9, 0x0045},
    {0x00CA, 0x00EA, 0x0045}, {0x00CB, 0x00EB, 0x0045},
    {0x00CC, 0x00EC, 0x0049}, {0x00CD, 0x00ED, 0x0049},
    {0x00CE, 0x00EE, 0x0049}, {0x00CF, 0x00EF, 0x0049},
    {0x00D0, 0x00F0, 0x00D0}, {0x00D1, 0x00F1, 0x004E},
    {0x00D2, 0x00F2, 0x004F}, {0x00D3, 0x00F3, 0x004F},
    {0x00D4, 0x00F4, 0x004F}, {0x00D5, 0x00F5, 0x004F},
    {0x00D6, 0x00F6, 0x004F}, {0x00F7, 0x00F7, 0x00F7},
    {0x00D8, 0x00F8, 0x00D8}, {0x00D9, 0x00F9, 0x0055},
    {0x00DA, 0x00FA, 0x0055}, {0x00DB, 0x00FB, 0x0055},
    {0x00DC, 0x00FC, 0x0055}, {0x00DD, 0x00FD, 0x0059},
    {0x00DE, 0x00FE, 0x00DE}, {0x0178, 0x00FF, 0x0059}};

extern ObUnicaseInfoChar utf8_plane01[];
extern ObUnicaseInfoChar utf8_plane02[];
extern ObUnicaseInfoChar utf8_plane03[];
extern ObUnicaseInfoChar utf8_plane04[];
extern ObUnicaseInfoChar utf8_plane05[];
extern ObUnicaseInfoChar utf8_plane1E[];
extern ObUnicaseInfoChar utf8_plane1F[];
extern ObUnicaseInfoChar utf8_plane21[];
extern ObUnicaseInfoChar utf8_plane24[];
extern ObUnicaseInfoChar utf8_planeFF[];

static ObUnicaseInfoChar *ob_unicase_pages_turkish[256] = {
    turk00,  utf8_plane01, utf8_plane02, utf8_plane03, utf8_plane04, utf8_plane05, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, utf8_plane1E, utf8_plane1F,
    nullptr, utf8_plane21, nullptr, nullptr, utf8_plane24, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, utf8_planeFF};

ObUnicaseInfo ob_unicase_turkish = {0xFFFF, ob_unicase_pages_turkish};

// Quaternary weight of katakana.
static constexpr int JA_KATA_QUAT_WEIGHT= 0x08;
// Quaternary weight of hiragana.
static constexpr int JA_HIRA_QUAT_WEIGHT= 0x02;
static const char ja_cldr_30[]=
  "&\\u309D <<<< \\u30FD"
  "&[before 3]\\u3041 <<<\\u3041|\\u30FC=\\u3042|\\u30FC=\\u304B|\\u30FC"
                       "=\\u3095|\\u30FC=\\u304C|\\u30FC=\\u3055|\\u30FC"
                       "=\\u3056|\\u30FC=\\u305F|\\u30FC=\\u3060|\\u30FC"
                       "=\\u306A|\\u30FC=\\u306F|\\u30FC=\\u3070|\\u30FC"
                       "=\\u3071|\\u30FC=\\u307E|\\u30FC=\\u3083|\\u30FC"
                       "=\\u3084|\\u30FC=\\u3089|\\u30FC=\\u308E|\\u30FC"
                       "=\\u308F|\\u30FC"
                    "<<<<\\u30A1|\\u30FC=\\uFF67|\\u30FC=\\u30A2|\\u30FC"
                       "=\\uFF71|\\u30FC=\\u30AB|\\u30FC=\\uFF76|\\u30FC"
                       "=\\u30AC|\\u30FC=\\u30B5|\\u30FC=\\uFF7B|\\u30FC"
                       "=\\u30B6|\\u30FC=\\u30BF|\\u30FC=\\uFF80|\\u30FC"
                       "=\\u30C0|\\u30FC=\\u30CA|\\u30FC=\\uFF85|\\u30FC"
                       "=\\u30CF|\\u30FC=\\uFF8A|\\u30FC=\\u31F5|\\u30FC"
                       "=\\u30D0|\\u30FC=\\u30D1|\\u30FC=\\u30DE|\\u30FC"
                       "=\\uFF8F|\\u30FC=\\u30E3|\\u30FC=\\uFF6C|\\u30FC"
                       "=\\u30E4|\\u30FC=\\uFF94|\\u30FC=\\u30E9|\\u30FC"
                       "=\\uFF97|\\u30FC=\\u31FB|\\u30FC=\\u30EE|\\u30FC"
                       "=\\u30EF|\\u30FC=\\uFF9C|\\u30FC=\\u30F5|\\u30FC"
                       "=\\u30F7|\\u30FC"
  "&[before 3]\\u3043 <<<\\u3043|\\u30FC=\\u3044|\\u30FC=\\u304D|\\u30FC"
                       "=\\u304E|\\u30FC=\\u3057|\\u30FC=\\u3058|\\u30FC"
                       "=\\u3061|\\u30FC=\\u3062|\\u30FC=\\u306B|\\u30FC"
                       "=\\u3072|\\u30FC=\\u3073|\\u30FC=\\u3074|\\u30FC"
                       "=\\u307F|\\u30FC=\\u308A|\\u30FC=\\u3090|\\u30FC"
                    "<<<<\\u30A3|\\u30FC=\\uFF68|\\u30FC=\\u30A4|\\u30FC"
                       "=\\uFF72|\\u30FC=\\u30AD|\\u30FC=\\uFF77|\\u30FC"
                       "=\\u30AE|\\u30FC=\\u30B7|\\u30FC=\\uFF7C|\\u30FC"
                       "=\\u31F1|\\u30FC=\\u30B8|\\u30FC=\\u30C1|\\u30FC"
                       "=\\uFF81|\\u30FC=\\u30C2|\\u30FC=\\u30CB|\\u30FC"
                       "=\\uFF86|\\u30FC=\\u30D2|\\u30FC=\\uFF8B|\\u30FC"
                       "=\\u31F6|\\u30FC=\\u30D3|\\u30FC=\\u30D4|\\u30FC"
                       "=\\u30DF|\\u30FC=\\uFF90|\\u30FC=\\u30EA|\\u30FC"
                       "=\\uFF98|\\u30FC=\\u31FC|\\u30FC=\\u30F0|\\u30FC"
                       "=\\u30F8|\\u30FC"
  "&[before 3]\\u3045 <<<\\u3045|\\u30FC=\\u3046|\\u30FC=\\u304F|\\u30FC"
                       "=\\u3050|\\u30FC=\\u3059|\\u30FC=\\u305A|\\u30FC"
                       "=\\u3063|\\u30FC=\\u3064|\\u30FC=\\u3065|\\u30FC"
                       "=\\u306C|\\u30FC=\\u3075|\\u30FC=\\u3076|\\u30FC"
                       "=\\u3077|\\u30FC=\\u3080|\\u30FC=\\u3085|\\u30FC"
                       "=\\u3086|\\u30FC=\\u308B|\\u30FC=\\u3094|\\u30FC"
                    "<<<<\\u30A5|\\u30FC=\\uFF69|\\u30FC=\\u30A6|\\u30FC"
                       "=\\uFF73|\\u30FC=\\u30AF|\\u30FC=\\uFF78|\\u30FC"
                       "=\\u31F0|\\u30FC=\\u30B0|\\u30FC=\\u30B9|\\u30FC"
                       "=\\uFF7D|\\u30FC=\\u31F2|\\u30FC=\\u30BA|\\u30FC"
                       "=\\u30C3|\\u30FC=\\uFF6F|\\u30FC=\\u30C4|\\u30FC"
                       "=\\uFF82|\\u30FC=\\u30C5|\\u30FC=\\u30CC|\\u30FC"
                       "=\\uFF87|\\u30FC=\\u31F4|\\u30FC=\\u30D5|\\u30FC"
                       "=\\uFF8C|\\u30FC=\\u31F7|\\u30FC=\\u30D6|\\u30FC"
                       "=\\u30D7|\\u30FC=\\u30E0|\\u30FC=\\uFF91|\\u30FC"
                       "=\\u31FA|\\u30FC=\\u30E5|\\u30FC=\\uFF6D|\\u30FC"
                       "=\\u30E6|\\u30FC=\\uFF95|\\u30FC=\\u30EB|\\u30FC"
                       "=\\uFF99|\\u30FC=\\u31FD|\\u30FC=\\u30F4|\\u30FC"
  "&[before 3]\\u3047 <<<\\u3047|\\u30FC=\\u3048|\\u30FC=\\u3051|\\u30FC"
                       "=\\u3096|\\u30FC=\\u3052|\\u30FC=\\u305B|\\u30FC"
                       "=\\u305C|\\u30FC=\\u3066|\\u30FC=\\u3067|\\u30FC"
                       "=\\u306D|\\u30FC=\\u3078|\\u30FC=\\u3079|\\u30FC"
                       "=\\u307A|\\u30FC=\\u3081|\\u30FC=\\u308C|\\u30FC"
                       "=\\u3091|\\u30FC"
                    "<<<<\\u30A7|\\u30FC=\\uFF6A|\\u30FC=\\u30A8|\\u30FC"
                       "=\\uFF74|\\u30FC=\\u30B1|\\u30FC=\\uFF79|\\u30FC"
                       "=\\u30B2|\\u30FC=\\u30BB|\\u30FC=\\uFF7E|\\u30FC"
                       "=\\u30BC|\\u30FC=\\u30C6|\\u30FC=\\uFF83|\\u30FC"
                       "=\\u30C7|\\u30FC=\\u30CD|\\u30FC=\\uFF88|\\u30FC"
                       "=\\u30D8|\\u30FC=\\uFF8D|\\u30FC=\\u31F8|\\u30FC"
                       "=\\u30D9|\\u30FC=\\u30DA|\\u30FC=\\u30E1|\\u30FC"
                       "=\\uFF92|\\u30FC=\\u30EC|\\u30FC=\\uFF9A|\\u30FC"
                       "=\\u31FE|\\u30FC=\\u30F1|\\u30FC=\\u30F6|\\u30FC"
                       "=\\u30F9|\\u30FC"
  "&[before 3]\\u3049 <<<\\u3049|\\u30FC=\\u304A|\\u30FC=\\u3053|\\u30FC"
                       "=\\u3054|\\u30FC=\\u305D|\\u30FC=\\u305E|\\u30FC"
                       "=\\u3068|\\u30FC=\\u3069|\\u30FC=\\u306E|\\u30FC"
                       "=\\u307B|\\u30FC=\\u307C|\\u30FC=\\u307D|\\u30FC"
                       "=\\u3082|\\u30FC=\\u3087|\\u30FC=\\u3088|\\u30FC"
                       "=\\u308D|\\u30FC=\\u3092|\\u30FC"
                    "<<<<\\u30A9|\\u30FC=\\uFF6B|\\u30FC=\\u30AA|\\u30FC"
                       "=\\uFF75|\\u30FC=\\u30B3|\\u30FC=\\uFF7A|\\u30FC"
                       "=\\u30B4|\\u30FC=\\u30BD|\\u30FC=\\uFF7F|\\u30FC"
                       "=\\u30BE|\\u30FC=\\u30C8|\\u30FC=\\uFF84|\\u30FC"
                       "=\\u31F3|\\u30FC=\\u30C9|\\u30FC=\\u30CE|\\u30FC"
                       "=\\uFF89|\\u30FC=\\u30DB|\\u30FC=\\uFF8E|\\u30FC"
                       "=\\u31F9|\\u30FC=\\u30DC|\\u30FC=\\u30DD|\\u30FC"
                       "=\\u30E2|\\u30FC=\\uFF93|\\u30FC=\\u30E7|\\u30FC"
                       "=\\uFF6E|\\u30FC=\\u30E8|\\u30FC=\\uFF96|\\u30FC"
                       "=\\u30ED|\\u30FC=\\uFF9B|\\u30FC=\\u31FF|\\u30FC"
                       "=\\u30F2|\\u30FC=\\uFF66|\\u30FC=\\u30FA|\\u30FC"
  "&[before 3]\\u3042 <<<\\u3042|\\u309D=\\u3041|\\u309D"
                    "<<<<\\u30A2|\\u30FD=\\uFF71|\\u30FD=\\u30A1|\\u30FD"
                       "=\\uFF67|\\u30FD"
  "&[before 3]\\u3044 <<<\\u3044|\\u309D=\\u3043|\\u309D"
                    "<<<<\\u30A4|\\u30FD=\\uFF72|\\u30FD=\\u30A3|\\u30FD"
                       "=\\uFF68|\\u30FD"
  "&[before 3]\\u3046 <<<\\u3046|\\u309D=\\u3045|\\u309D=\\u3094|\\u309D"
                       "=\\u3046|\\u309E/\\u3099"
                       "=\\u3045|\\u309E/\\u3099"
                       "=\\u3094|\\u309E/\\u3099"
                    "<<<<\\u30A6|\\u30FD=\\uFF73|\\u30FD=\\u30A5|\\u30FD"
                       "=\\uFF69|\\u30FD=\\u30F4|\\u30FD"
                       "=\\u30A6|\\u30FE/\\u3099"
                       "=\\uFF73|\\u30FE/\\u3099"
                       "=\\u30A5|\\u30FE/\\u3099"
                       "=\\uFF69|\\u30FE/\\u3099"
                       "=\\u30F4|\\u30FE/\\u3099"
  "&[before 3]\\u3048 <<<\\u3048|\\u309D=\\u3047|\\u309D"
                    "<<<<\\u30A8|\\u30FD=\\uFF74|\\u30FD=\\u30A7|\\u30FD"
                       "=\\uFF6A|\\u30FD"
  "&[before 3]\\u304A <<<\\u304A|\\u309D=\\u3049|\\u309D"
                    "<<<<\\u30AA|\\u30FD=\\uFF75|\\u30FD=\\u30A9|\\u30FD"
                       "=\\uFF6B|\\u30FD"
  "&[before 3]\\u304B <<<\\u304B|\\u309D=\\u3095|\\u309D"
                    "<<<<\\u30AB|\\u30FD=\\uFF76|\\u30FD=\\u30F5|\\u30FD"
  "&[before 3]\\u304C <<<\\u304C|\\u309D <<<<\\u30AC|\\u30FD"
  "&[before 3]\\u304D <<<\\u304D|\\u309D=\\u304E|\\u309D"
                       "=\\u304D|\\u309E/\\u3099"
                       "=\\u304E|\\u309E/\\u3099"
                    "<<<<\\u30AD|\\u30FD=\\uFF77|\\u30FD=\\u30AE|\\u30FD"
                       "=\\u30AD|\\u30FE/\\u3099"
                       "=\\uFF77|\\u30FE/\\u3099"
                       "=\\u30AE|\\u30FE/\\u3099"
  "&[before 3]\\u304F <<<\\u304F|\\u309D=\\u3050|\\u309D"
                       "=\\u304F|\\u309E/\\u3099"
                       "=\\u3050|\\u309E/\\u3099"
                    "<<<<\\u30AF|\\u30FD=\\uFF78|\\u30FD=\\u31F0|\\u30FD"
                       "=\\u30B0|\\u30FD=\\u30AF|\\u30FE/\\u3099"
                       "=\\uFF78|\\u30FE/\\u3099"
                       "=\\u31F0|\\u30FE/\\u3099"
                       "=\\u30B0|\\u30FE/\\u3099"
  "&[before 3]\\u3051 <<<\\u3051|\\u309D=\\u3096|\\u309D"
                    "<<<<\\u30B1|\\u30FD=\\uFF79|\\u30FD=\\u30F6|\\u30FD"
  "&[before 3]\\u3052 <<<\\u3052|\\u309D <<<<\\u30B2|\\u30FD"
  "&[before 3]\\u3053 <<<\\u3053|\\u309D=\\u3054|\\u309D"
                       "=\\u3053|\\u309E/\\u3099"
                       "=\\u3054|\\u309E/\\u3099"
                    "<<<<\\u30B3|\\u30FD=\\uFF7A|\\u30FD=\\u30B4|\\u30FD"
                       "=\\u30B3|\\u30FE/\\u3099"
                       "=\\uFF7A|\\u30FE/\\u3099"
                       "=\\u30B4|\\u30FE/\\u3099"
  "&[before 3]\\u3055 <<<\\u3055|\\u309D=\\u3056|\\u309D"
                       "=\\u3055|\\u309E/\\u3099"
                       "=\\u3056|\\u309E/\\u3099"
                    "<<<<\\u30B5|\\u30FD=\\uFF7B|\\u30FD=\\u30B6|\\u30FD"
                       "=\\u30B5|\\u30FE/\\u3099"
                       "=\\uFF7B|\\u30FE/\\u3099"
                       "=\\u30B6|\\u30FE/\\u3099"
  "&[before 3]\\u3057 <<<\\u3057|\\u309D=\\u3058|\\u309D"
                       "=\\u3057|\\u309E/\\u3099"
                       "=\\u3058|\\u309E/\\u3099"
                    "<<<<\\u30B7|\\u30FD=\\uFF7C|\\u30FD=\\u31F1|\\u30FD"
                       "=\\u30B8|\\u30FD=\\u30B7|\\u30FE/\\u3099"
                       "=\\uFF7C|\\u30FE/\\u3099"
                       "=\\u31F1|\\u30FE/\\u3099"
                       "=\\u30B8|\\u30FE/\\u3099"
  "&[before 3]\\u3059 <<<\\u3059|\\u309D=\\u305A|\\u309D"
                       "=\\u3059|\\u309E/\\u3099"
                       "=\\u305A|\\u309E/\\u3099"
                    "<<<<\\u30B9|\\u30FD=\\uFF7D|\\u30FD=\\u31F2|\\u30FD"
                       "=\\u30BA|\\u30FD=\\u30B9|\\u30FE/\\u3099"
                       "=\\uFF7D|\\u30FE/\\u3099"
                       "=\\u31F2|\\u30FE/\\u3099"
                       "=\\u30BA|\\u30FE/\\u3099"
  "&[before 3]\\u305B <<<\\u305B|\\u309D=\\u305C|\\u309D"
                       "=\\u305B|\\u309E/\\u3099"
                       "=\\u305C|\\u309E/\\u3099"
                    "<<<<\\u30BB|\\u30FD=\\uFF7E|\\u30FD=\\u30BC|\\u30FD"
                       "=\\u30BB|\\u30FE/\\u3099"
                       "=\\uFF7E|\\u30FE/\\u3099"
                       "=\\u30BC|\\u30FE/\\u3099"
  "&[before 3]\\u305D <<<\\u305D|\\u309D=\\u305E|\\u309D"
                       "=\\u305D|\\u309E/\\u3099"
                       "=\\u305E|\\u309E/\\u3099"
                    "<<<<\\u30BD|\\u30FD=\\uFF7F|\\u30FD=\\u30BE|\\u30FD"
                       "=\\u30BD|\\u30FE/\\u3099"
                       "=\\uFF7F|\\u30FE/\\u3099"
                       "=\\u30BE|\\u30FE/\\u3099"
  "&[before 3]\\u305F <<<\\u305F|\\u309D=\\u3060|\\u309D"
                       "=\\u305F|\\u309E/\\u3099"
                       "=\\u3060|\\u309E/\\u3099"
                    "<<<<\\u30BF|\\u30FD=\\uFF80|\\u30FD=\\u30C0|\\u30FD"
                       "=\\u30BF|\\u30FE/\\u3099"
                       "=\\uFF80|\\u30FE/\\u3099"
                       "=\\u30C0|\\u30FE/\\u3099"
  "&[before 3]\\u3061 <<<\\u3061|\\u309D=\\u3062|\\u309D"
                       "=\\u3061|\\u309E/\\u3099"
                       "=\\u3062|\\u309E/\\u3099"
                    "<<<<\\u30C1|\\u30FD=\\uFF81|\\u30FD=\\u30C2|\\u30FD"
                       "=\\u30C1|\\u30FE/\\u3099"
                       "=\\uFF81|\\u30FE/\\u3099"
                       "=\\u30C2|\\u30FE/\\u3099"
  "&[before 3]\\u3064 <<<\\u3064|\\u309D=\\u3063|\\u309D=\\u3065|\\u309D"
                       "=\\u3064|\\u309E/\\u3099"
                       "=\\u3065|\\u309E/\\u3099"
                       "=\\u3064|\\u309D=\\u3063|\\u309E/\\u3099"
                       "=\\u3064|\\u309E/\\u3099"
                    "<<<<\\u30C4|\\u30FD=\\uFF82|\\u30FD=\\u30C3|\\u30FD"
                       "=\\uFF6F|\\u30FD=\\u30C5|\\u30FD"
                       "=\\u30C4|\\u30FE/\\u3099"
                       "=\\uFF82|\\u30FE/\\u3099"
                       "=\\u30C5|\\u30FE/\\u3099=\\u30C4|\\u30FD"
                       "=\\uFF82|\\u30FD=\\u30C3|\\u30FE/\\u3099"
                       "=\\uFF6F|\\u30FE/\\u3099"
                       "=\\u30C4|\\u30FE/\\u3099"
                       "=\\uFF82|\\u30FE/\\u3099"
  "&[before 3]\\u3066 <<<\\u3066|\\u309D=\\u3067|\\u309D"
                       "=\\u3066|\\u309E/\\u3099"
                       "=\\u3067|\\u309E/\\u3099"
                    "<<<<\\u30C6|\\u30FD=\\uFF83|\\u30FD=\\u30C7|\\u30FD"
                       "=\\u30C6|\\u30FE/\\u3099"
                       "=\\uFF83|\\u30FE/\\u3099"
                       "=\\u30C7|\\u30FE/\\u3099"
  "&[before 3]\\u3068 <<<\\u3068|\\u309D=\\u3069|\\u309D"
                       "=\\u3068|\\u309E/\\u3099"
                       "=\\u3069|\\u309E/\\u3099"
                    "<<<<\\u30C8|\\u30FD=\\uFF84|\\u30FD=\\u31F3|\\u30FD"
                       "=\\u30C9|\\u30FD=\\u30C8|\\u30FE/\\u3099"
                       "=\\uFF84|\\u30FE/\\u3099"
                       "=\\u31F3|\\u30FE/\\u3099"
                       "=\\u30C9|\\u30FE/\\u3099"
  "&[before 3]\\u306A <<<\\u306A|\\u309D <<<<\\u30CA|\\u30FD=\\uFF85|\\u30FD"
  "&[before 3]\\u306B <<<\\u306B|\\u309D <<<<\\u30CB|\\u30FD=\\uFF86|\\u30FD"
  "&[before 3]\\u306C <<<\\u306C|\\u309D <<<<\\u30CC|\\u30FD=\\uFF87|\\u30FD"
                       "=\\u31F4|\\u30FD"
  "&[before 3]\\u306D <<<\\u306D|\\u309D <<<<\\u30CD|\\u30FD=\\uFF88|\\u30FD"
  "&[before 3]\\u306E <<<\\u306E|\\u309D <<<<\\u30CE|\\u30FD=\\uFF89|\\u30FD"
  "&[before 3]\\u306F <<<\\u306F|\\u309D=\\u3070|\\u309D"
                       "=\\u306F|\\u309E/\\u3099"
                       "=\\u3070|\\u309E/\\u3099"
                       "=\\u3071|\\u309D=\\u3071|\\u309E/\\u3099"
                    "<<<<\\u30CF|\\u30FD=\\uFF8A|\\u30FD=\\u31F5|\\u30FD"
                       "=\\u30D0|\\u30FD=\\u30CF|\\u30FE/\\u3099"
                       "=\\uFF8A|\\u30FE/\\u3099"
                       "=\\u31F5|\\u30FE/\\u3099"
                       "=\\u30D0|\\u30FE/\\u3099=\\u30D1|\\u30FD"
                       "=\\u30D1|\\u30FE/\\u3099"
  "&[before 3]\\u3072 <<<\\u3072|\\u309D=\\u3073|\\u309D"
                       "=\\u3072|\\u309E/\\u3099"
                       "=\\u3073|\\u309E/\\u3099"
                       "=\\u3074|\\u309D=\\u3074|\\u309E/\\u3099"
                    "<<<<\\u30D2|\\u30FD=\\uFF8B|\\u30FD=\\u31F6|\\u30FD"
                       "=\\u30D3|\\u30FD=\\u30D2|\\u30FE/\\u3099"
                       "=\\uFF8B|\\u30FE/\\u3099"
                       "=\\u31F6|\\u30FE/\\u3099"
                       "=\\u30D3|\\u30FE/\\u3099=\\u30D4|\\u30FD"
                       "=\\u30D4|\\u30FE/\\u3099"
  "&[before 3]\\u3075 <<<\\u3075|\\u309D=\\u3076|\\u309D"
                       "=\\u3075|\\u309E/\\u3099"
                       "=\\u3076|\\u309E/\\u3099"
                       "=\\u3077|\\u309D=\\u3077|\\u309E/\\u3099"
                    "<<<<\\u30D5|\\u30FD=\\uFF8C|\\u30FD=\\u31F7|\\u30FD"
                       "=\\u30D6|\\u30FD=\\u30D5|\\u30FE/\\u3099"
                       "=\\uFF8C|\\u30FE/\\u3099"
                       "=\\u31F7|\\u30FE/\\u3099"
                       "=\\u30D6|\\u30FE/\\u3099=\\u30D7|\\u30FD"
                       "=\\u30D7|\\u30FE/\\u3099"
  "&[before 3]\\u3078 <<<\\u3078|\\u309D=\\u3079|\\u309D"
                       "=\\u3078|\\u309E/\\u3099"
                       "=\\u3079|\\u309E/\\u3099"
                       "=\\u307A|\\u309D=\\u307A|\\u309E/\\u3099"
                    "<<<<\\u30D8|\\u30FD=\\uFF8D|\\u30FD=\\u31F8|\\u30FD"
                       "=\\u30D9|\\u30FD=\\u30D8|\\u30FE/\\u3099"
                       "=\\uFF8D|\\u30FE/\\u3099"
                       "=\\u31F8|\\u30FE/\\u3099"
                       "=\\u30D9|\\u30FE/\\u3099=\\u30DA|\\u30FD"
                       "=\\u30DA|\\u30FE/\\u3099"
  "&[before 3]\\u307B <<<\\u307B|\\u309D=\\u307C|\\u309D"
                       "=\\u307B|\\u309E/\\u3099"
                       "=\\u307C|\\u309E/\\u3099"
                       "=\\u307D|\\u309D=\\u307D|\\u309E/\\u3099"
                    "<<<<\\u30DB|\\u30FD=\\uFF8E|\\u30FD=\\u31F9|\\u30FD"
                       "=\\u30DC|\\u30FD=\\u30DB|\\u30FE/\\u3099"
                       "=\\uFF8E|\\u30FE/\\u3099"
                       "=\\u31F9|\\u30FE/\\u3099"
                       "=\\u30DC|\\u30FE/\\u3099=\\u30DD|\\u30FD"
                       "=\\u30DD|\\u30FE/\\u3099"
  "&[before 3]\\u307E <<<\\u307E|\\u309D <<<<\\u30DE|\\u30FD=\\uFF8F|\\u30FD"
  "&[before 3]\\u307F <<<\\u307F|\\u309D <<<<\\u30DF|\\u30FD=\\uFF90|\\u30FD"
  "&[before 3]\\u3080 <<<\\u3080|\\u309D <<<<\\u30E0|\\u30FD=\\uFF91|\\u30FD"
                       "=\\u31FA|\\u30FD"
  "&[before 3]\\u3081 <<<\\u3081|\\u309D <<<<\\u30E1|\\u30FD=\\uFF92|\\u30FD"
  "&[before 3]\\u3082 <<<\\u3082|\\u309D <<<<\\u30E2|\\u30FD=\\uFF93|\\u30FD"
  "&[before 3]\\u3084 <<<\\u3084|\\u309D=\\u3083|\\u309D <<<<\\u30E4|\\u30FD"
                       "=\\uFF94|\\u30FD=\\u30E3|\\u30FD=\\uFF6C|\\u30FD"
  "&[before 3]\\u3086 <<<\\u3086|\\u309D=\\u3085|\\u309D <<<<\\u30E6|\\u30FD"
                       "=\\uFF95|\\u30FD=\\u30E5|\\u30FD=\\uFF6D|\\u30FD"
  "&[before 3]\\u3088 <<<\\u3088|\\u309D=\\u3087|\\u309D <<<<\\u30E8|\\u30FD"
                       "=\\uFF96|\\u30FD=\\u30E7|\\u30FD=\\uFF6E|\\u30FD"
  "&[before 3]\\u3089 <<<\\u3089|\\u309D <<<<\\u30E9|\\u30FD=\\uFF97|\\u30FD"
                       "=\\u31FB|\\u30FD"
  "&[before 3]\\u308A <<<\\u308A|\\u309D <<<<\\u30EA|\\u30FD=\\uFF98|\\u30FD"
                       "=\\u31FC|\\u30FD"
  "&[before 3]\\u308B <<<\\u308B|\\u309D <<<<\\u30EB|\\u30FD=\\uFF99|\\u30FD"
                       "=\\u31FD|\\u30FD"
  "&[before 3]\\u308C <<<\\u308C|\\u309D <<<<\\u30EC|\\u30FD=\\uFF9A|\\u30FD"
                       "=\\u31FE|\\u30FD"
  "&[before 3]\\u308D <<<\\u308D|\\u309D <<<<\\u30ED|\\u30FD=\\uFF9B|\\u30FD"
                       "=\\u31FF|\\u30FD"
  "&[before 3]\\u308F <<<\\u308F|\\u309D=\\u308E|\\u309D"
                       "=\\u308F|\\u309E/\\u3099"
                       "=\\u308E|\\u309E/\\u3099"
                    "<<<<\\u30EF|\\u30FD=\\uFF9C|\\u30FD=\\u30EE|\\u30FD"
                       "=\\u30F7|\\u30FD=\\u30EF|\\u30FE/\\u3099"
                       "=\\uFF9C|\\u30FE/\\u3099"
                       "=\\u30F7|\\u30FE/\\u3099"
                       "=\\u30EE|\\u30FE/\\u3099"
  "&[before 3]\\u3090 <<<\\u3090|\\u309D=\\u3090|\\u309E/\\u3099"
                    "<<<<\\u30F0|\\u30FD=\\u30F8|\\u30FD"
                       "=\\u30F0|\\u30FE/\\u3099"
                       "=\\u30F8|\\u30FE/\\u3099"
  "&[before 3]\\u3091 <<<\\u3091|\\u309D=\\u3091|\\u309E/\\u3099"
                    "<<<<\\u30F1|\\u30FD=\\u30F9|\\u30FD"
                       "=\\u30F1|\\u30FE/\\u3099"
                       "=\\u30F9|\\u30FE/\\u3099"
  "&[before 3]\\u3092 <<<\\u3092|\\u309D=\\u3092|\\u309E/\\u3099"
                    "<<<<\\u30F2|\\u30FD=\\uFF66|\\u30FD=\\u30FA|\\u30FD"
                       "=\\u30F2|\\u30FE/\\u3099"
                       "=\\uFF66|\\u30FE/\\u3099"
                       "=\\u30FA|\\u30FE/\\u3099"
  "&[before 3]\\u3093 <<<\\u3093|\\u309D <<<<\\u30F3|\\u30FD=\\uFF9D|\\u30FD"
  "&\\u3041 <<<<\\u30A1=\\uFF67"
  "&\\u3042 <<<<\\u30A2=\\uFF71"
  "&\\u3043 <<<<\\u30A3=\\uFF68"
  "&\\u3044 <<<<\\u30A4=\\uFF72"
  "&\\u3045 <<<<\\u30A5=\\uFF69"
  "&\\u3046 <<<<\\u30A6=\\uFF73"
  "&\\u3047 <<<<\\u30A7=\\uFF6A"
  "&\\u3048 <<<<\\u30A8=\\uFF74"
  "&\\u3049 <<<<\\u30A9=\\uFF6B"
  "&\\u304A <<<<\\u30AA=\\uFF75"
  "&\\u304B <<<<\\u30AB=\\uFF76"
  "&\\u304D <<<<\\u30AD=\\uFF77"
  "&\\u304F <<<<\\u30AF=\\uFF78"
  "&\\u3051 <<<<\\u30B1=\\uFF79"
  "&\\u3053 <<<<\\u30B3=\\uFF7A"
  "&\\u3055 <<<<\\u30B5=\\uFF7B"
  "&\\u3057 <<<<\\u30B7=\\uFF7C"
  "&\\u3059 <<<<\\u30B9=\\uFF7D"
  "&\\u305B <<<<\\u30BB=\\uFF7E"
  "&\\u305D <<<<\\u30BD=\\uFF7F"
  "&\\u305F <<<<\\u30BF=\\uFF80"
  "&\\u3061 <<<<\\u30C1=\\uFF81"
  "&\\u3063 <<<<\\u30C3=\\uFF6F"
  "&\\u3064 <<<<\\u30C4=\\uFF82"
  "&\\u3066 <<<<\\u30C6=\\uFF83"
  "&\\u3068 <<<<\\u30C8=\\uFF84"
  "&\\u306A <<<<\\u30CA=\\uFF85"
  "&\\u306B <<<<\\u30CB=\\uFF86"
  "&\\u306C <<<<\\u30CC=\\uFF87"
  "&\\u306D <<<<\\u30CD=\\uFF88"
  "&\\u306E <<<<\\u30CE=\\uFF89"
  "&\\u306F <<<<\\u30CF=\\uFF8A"
  "&\\u3072 <<<<\\u30D2=\\uFF8B"
  "&\\u3075 <<<<\\u30D5=\\uFF8C"
  "&\\u3078 <<<<\\u30D8=\\uFF8D"
  "&\\u307B <<<<\\u30DB=\\uFF8E"
  "&\\u307E <<<<\\u30DE=\\uFF8F"
  "&\\u307F <<<<\\u30DF=\\uFF90"
  "&\\u3080 <<<<\\u30E0=\\uFF91"
  "&\\u3081 <<<<\\u30E1=\\uFF92"
  "&\\u3082 <<<<\\u30E2=\\uFF93"
  "&\\u3083 <<<<\\u30E3=\\uFF6C"
  "&\\u3084 <<<<\\u30E4=\\uFF94"
  "&\\u3085 <<<<\\u30E5=\\uFF6D"
  "&\\u3086 <<<<\\u30E6=\\uFF95"
  "&\\u3087 <<<<\\u30E7=\\uFF6E"
  "&\\u3088 <<<<\\u30E8=\\uFF96"
  "&\\u3089 <<<<\\u30E9=\\uFF97"
  "&\\u308A <<<<\\u30EA=\\uFF98"
  "&\\u308B <<<<\\u30EB=\\uFF99"
  "&\\u308C <<<<\\u30EC=\\uFF9A"
  "&\\u308D <<<<\\u30ED=\\uFF9B"
  "&\\u308E <<<<\\u30EE"
  "&\\u308F <<<<\\u30EF=\\uFF9C"
  "&\\u3090 <<<<\\u30F0"
  "&\\u3091 <<<<\\u30F1"
  "&\\u3092 <<<<\\u30F2=\\uFF66"
  "&\\u3093 <<<<\\u30F3=\\uFF9D"
  "&\\u3095 <<<<\\u30F5"
  "&\\u3096 <<<<\\u30F6"
  "&\\u3088\\u308A <<\\u309F"
  "&\\u30B3\\u30C8 <<\\u30FF"
  "&\\u0020=\\u3000=\\uFFE3"
  "&\\u0021=\\uFF01"
  "&\\u0022=\\uFF02"
  "&\\u0023=\\uFF03"
  "&\\u0024=\\uFF04"
  "&\\u0025=\\uFF05"
  "&\\u0026=\\uFF06"
  "&\\u0027=\\uFF07"
  "&\\u0028=\\uFF08"
  "&\\u0029=\\uFF09"
  "&\\u002A=\\uFF0A"
  "&\\u002B=\\uFF0B"
  "&\\u002C=\\uFF0C"
  "&\\u002D=\\uFF0D"
  "&\\u002E=\\uFF0E"
  "&\\u002F=\\uFF0F"
  "&0=\\uFF10"
  "&1=\\uFF11"
  "&2=\\uFF12"
  "&3=\\uFF13"
  "&4=\\uFF14"
  "&5=\\uFF15"
  "&6=\\uFF16"
  "&7=\\uFF17"
  "&8=\\uFF18"
  "&9=\\uFF19"
  "&\\u003A=\\uFF1A"
  "&\\u003B=\\uFF1B"
  "&\\u003C=\\uFF1C"
  "&\\u003D=\\uFF1D"
  "&\\u003E=\\uFF1E"
  "&\\u003F=\\uFF1F"
  "&\\u0040=\\uFF20"
  "&A=\\uFF21"
  "&B=\\uFF22"
  "&C=\\uFF23"
  "&D=\\uFF24"
  "&E=\\uFF25"
  "&F=\\uFF26"
  "&G=\\uFF27"
  "&H=\\uFF28"
  "&I=\\uFF29"
  "&J=\\uFF2A"
  "&K=\\uFF2B"
  "&L=\\uFF2C"
  "&M=\\uFF2D"
  "&N=\\uFF2E"
  "&O=\\uFF2F"
  "&P=\\uFF30"
  "&Q=\\uFF31"
  "&R=\\uFF32"
  "&S=\\uFF33"
  "&T=\\uFF34"
  "&U=\\uFF35"
  "&V=\\uFF36"
  "&W=\\uFF37"
  "&X=\\uFF38"
  "&Y=\\uFF39"
  "&Z=\\uFF3A"
  "&\\u005B=\\uFF3B"
  "&\\u005C=\\uFF3C "
  "&\\u005D=\\uFF3D"
  "&\\u005E=\\uFF3E"
  "&\\u005F=\\uFF3F"
  "&\\u0060=\\uFF40"
  "&a=\\uFF41"
  "&b=\\uFF42"
  "&c=\\uFF43"
  "&d=\\uFF44"
  "&e=\\uFF45"
  "&f=\\uFF46"
  "&g=\\uFF47"
  "&h=\\uFF48"
  "&i=\\uFF49"
  "&j=\\uFF4A"
  "&k=\\uFF4B"
  "&l=\\uFF4C"
  "&m=\\uFF4D"
  "&n=\\uFF4E"
  "&o=\\uFF4F"
  "&p=\\uFF50"
  "&q=\\uFF51"
  "&r=\\uFF52"
  "&s=\\uFF53"
  "&t=\\uFF54"
  "&u=\\uFF55"
  "&v=\\uFF56"
  "&w=\\uFF57"
  "&x=\\uFF58"
  "&y=\\uFF59"
  "&z=\\uFF5A"
  "&\\u007B=\\uFF5B"
  "&\\u007C=\\uFF5C"
  "&\\u007D=\\uFF5D"
  "&\\u007E=\\uFF5E"
  "&\\u00A2=\\uFFE0"
  "&\\u00A3=\\uFFE1"
  "&\\u00A5=\\uFFE5"
  "&\\u00A6=\\uFFE4"
  "&\\u00AC=\\uFFE2"
  "&\\u1100=\\uFFA1=\\u3131"
  "&\\u1101=\\uFFA2=\\u3132"
  "&\\u1102=\\uFFA4=\\u3134"
  "&\\u1103=\\uFFA7=\\u3137"
  "&\\u1104=\\uFFA8=\\u3138"
  "&\\u1105=\\uFFA9=\\u3139"
  "&\\u1106=\\uFFB1=\\u3141"
  "&\\u1107=\\uFFB2=\\u3142"
  "&\\u1108=\\uFFB3=\\u3143"
  "&\\u1109=\\uFFB5=\\u3145"
  "&\\u110A=\\uFFB6=\\u3146"
  "&\\u110B=\\uFFB7=\\u3147"
  "&\\u110C=\\uFFB8=\\u3148"
  "&\\u110D=\\uFFB9=\\u3149"
  "&\\u110E=\\uFFBA=\\u314A"
  "&\\u110F=\\uFFBB=\\u314B"
  "&\\u1110=\\uFFBC=\\u314C"
  "&\\u1111=\\uFFBD=\\u314D"
  "&\\u1112=\\uFFBE=\\u314E"
  "&\\u111A=\\uFFB0=\\u3140"
  "&\\u1121=\\uFFB4=\\u3144"
  "&\\u1160=\\uFFA0=\\u3164"
  "&\\u1161=\\uFFC2=\\u314F"
  "&\\u1162=\\uFFC3=\\u3150"
  "&\\u1163=\\uFFC4=\\u3151"
  "&\\u1164=\\uFFC5=\\u3152"
  "&\\u1165=\\uFFC6=\\u3153"
  "&\\u1166=\\uFFC7=\\u3154"
  "&\\u1167=\\uFFCA=\\u3155"
  "&\\u1168=\\uFFCB=\\u3156"
  "&\\u1169=\\uFFCC=\\u3157"
  "&\\u116A=\\uFFCD=\\u3158"
  "&\\u116B=\\uFFCE=\\u3159"
  "&\\u116C=\\uFFCF=\\u315A"
  "&\\u116D=\\uFFD2=\\u315B"
  "&\\u116E=\\uFFD3=\\u315C"
  "&\\u116F=\\uFFD4=\\u315D"
  "&\\u1170=\\uFFD5=\\u315E"
  "&\\u1171=\\uFFD6=\\u315F"
  "&\\u1172=\\uFFD7=\\u3160"
  "&\\u1173=\\uFFDA=\\u3161"
  "&\\u1174=\\uFFDB=\\u3162"
  "&\\u1175=\\uFFDC=\\u3163"
  "&\\u11AA=\\uFFA3=\\u3133"
  "&\\u11AC=\\uFFA5=\\u3135"
  "&\\u11AD=\\uFFA6=\\u3136"
  "&\\u11B0=\\uFFAA=\\u313A"
  "&\\u11B1=\\uFFAB=\\u313B"
  "&\\u11B2=\\uFFAC=\\u313C"
  "&\\u11B3=\\uFFAD=\\u313D"
  "&\\u11B4=\\uFFAE=\\u313E"
  "&\\u11B5=\\uFFAF=\\u313F"
  "&\\u20A9=\\uFFE6"
  "&\\u2190=\\uFFE9"
  "&\\u2191=\\uFFEA"
  "&\\u2192=\\uFFEB"
  "&\\u2193=\\uFFEC"
  "&\\u2502=\\uFFE8"
  "&\\u25A0=\\uFFED"
  "&\\u25CB=\\uFFEE"
  "&\\u3001=\\uFF64"
  "&\\u3002=\\uFF61"
  "&\\u300C=\\uFF62"
  "&\\u300D=\\uFF63";

/*
  Below variables are defined in separate .cc file, generated by uca9dump at
  build-time for the Japanese collations.
 */
extern uint16 *ja_han_pages[];
extern const int MIN_JA_HAN_PAGE;
extern const int MAX_JA_HAN_PAGE;

static const char zh_cldr_30[] =
    "&[before 2]a<<\\u0101<<<\\u0100<<\\u00E1<<<\\u00C1<<\\u01CE<<<\\u01CD"
    "<<\\u00E0<<<\\u00C0"
    "&[before 2]e<<\\u0113<<<\\u0112<<\\u00E9<<<\\u00C9<<\\u011B<<<\\u011A"
    "<<\\u00E8<<<\\u00C8"
    "&e<<e\\u0302\\u0304<<<E\\u0302\\u0304<<e\\u0302\\u0301<<<E\\u0302\\u0301"
    "<<e\\u0302\\u030C<<<E\\u0302\\u030C<<e\\u0302\\u0300<<<E\\u0302\\u0300"
    "&[before 2]i<<\\u012B<<<\\u012A<<\\u00ED<<<\\u00CD<<\\u01D0<<<\\u01CF"
    "<<\\u00EC<<<\\u00CC"
    "&[before 2]m<<m\\u0304<<<M\\u0304<<\\u1E3F<<<\\u1E3E<<m\\u030C"
    "<<<M\\u030C<<m\\u0300<<<M\\u0300"
    "&[before 2]n<<n\\u0304<<<N\\u0304<<\\u0144<<<\\u0143<<\\u0148<<<\\u0147"
    "<<\\u01F9<<<\\u01F8"
    "&[before 2]o<<\\u014D<<<\\u014C<<\\u00F3<<<\\u00D3<<\\u01D2<<<\\u01D1"
    "<<\\u00F2<<<\\u00D2"
    "&[before 2]u<<\\u016B<<<\\u016A<<\\u00FA<<<\\u00DA<<\\u01D4<<<\\u01D3"
    "<<\\u00F9<<<\\u00D9"
    "&U<<\\u01D6<<<\\u01D5<<\\u01D8<<<\\u01D7<<\\u01DA<<<\\u01D9<<\\u01DC"
    "<<<\\u01DB<<\\u00FC<<<\\u00DC"
    "&(\\u4E00)<<<\\u3220"
    "&(\\u4E03)<<<\\u3226"
    "&(\\u4E09)<<<\\u3222"
    "&(\\u4E5D)<<<\\u3228"
    "&(\\u4E8C)<<<\\u3221"
    "&(\\u4E94)<<<\\u3224"
    "&(\\u4EE3)<<<\\u3239"
    "&(\\u4F01)<<<\\u323D"
    "&(\\u4F11)<<<\\u3241"
    "&(\\u516B)<<<\\u3227"
    "&(\\u516D)<<<\\u3225"
    "&(\\u52B4)<<<\\u3238"
    "&(\\u5341)<<<\\u3229"
    "&(\\u5354)<<<\\u323F"
    "&(\\u540D)<<<\\u3234"
    "&(\\u547C)<<<\\u323A"
    "&(\\u56DB)<<<\\u3223"
    "&(\\u571F)<<<\\u322F"
    "&(\\u5B66)<<<\\u323B"
    "&(\\u65E5)<<<\\u3230"
    "&(\\u6708)<<<\\u322A"
    "&(\\u6709)<<<\\u3232"
    "&(\\u6728)<<<\\u322D"
    "&(\\u682A)<<<\\u3231"
    "&(\\u6C34)<<<\\u322C"
    "&(\\u706B)<<<\\u322B"
    "&(\\u7279)<<<\\u3235"
    "&(\\u76E3)<<<\\u323C"
    "&(\\u793E)<<<\\u3233"
    "&(\\u795D)<<<\\u3237"
    "&(\\u796D)<<<\\u3240"
    "&(\\u81EA)<<<\\u3242"
    "&(\\u81F3)<<<\\u3243"
    "&(\\u8CA1)<<<\\u3236"
    "&(\\u8CC7)<<<\\u323E"
    "&(\\u91D1)<<<\\u322E"
    "&0\\u70B9<<<\\u3358"
    "&10\\u65E5<<<\\u33E9"
    "&10\\u6708<<<\\u32C9"
    "&10\\u70B9<<<\\u3362"
    "&11\\u65E5<<<\\u33EA"
    "&11\\u6708<<<\\u32CA"
    "&11\\u70B9<<<\\u3363"
    "&12\\u65E5<<<\\u33EB"
    "&12\\u6708<<<\\u32CB"
    "&12\\u70B9<<<\\u3364"
    "&13\\u65E5<<<\\u33EC"
    "&13\\u70B9<<<\\u3365"
    "&14\\u65E5<<<\\u33ED"
    "&14\\u70B9<<<\\u3366"
    "&15\\u65E5<<<\\u33EE"
    "&15\\u70B9<<<\\u3367"
    "&16\\u65E5<<<\\u33EF"
    "&16\\u70B9<<<\\u3368"
    "&17\\u65E5<<<\\u33F0"
    "&17\\u70B9<<<\\u3369"
    "&18\\u65E5<<<\\u33F1"
    "&18\\u70B9<<<\\u336A"
    "&19\\u65E5<<<\\u33F2"
    "&19\\u70B9<<<\\u336B"
    "&1\\u65E5<<<\\u33E0"
    "&1\\u6708<<<\\u32C0"
    "&1\\u70B9<<<\\u3359"
    "&20\\u65E5<<<\\u33F3"
    "&20\\u70B9<<<\\u336C"
    "&21\\u65E5<<<\\u33F4"
    "&21\\u70B9<<<\\u336D"
    "&22\\u65E5<<<\\u33F5"
    "&22\\u70B9<<<\\u336E"
    "&23\\u65E5<<<\\u33F6"
    "&23\\u70B9<<<\\u336F"
    "&24\\u65E5<<<\\u33F7"
    "&24\\u70B9<<<\\u3370"
    "&25\\u65E5<<<\\u33F8"
    "&26\\u65E5<<<\\u33F9"
    "&27\\u65E5<<<\\u33FA"
    "&28\\u65E5<<<\\u33FB"
    "&29\\u65E5<<<\\u33FC"
    "&2\\u65E5<<<\\u33E1"
    "&2\\u6708<<<\\u32C1"
    "&2\\u70B9<<<\\u335A"
    "&30\\u65E5<<<\\u33FD"
    "&31\\u65E5<<<\\u33FE"
    "&3\\u65E5<<<\\u33E2"
    "&3\\u6708<<<\\u32C2"
    "&3\\u70B9<<<\\u335B"
    "&4\\u65E5<<<\\u33E3"
    "&4\\u6708<<<\\u32C3"
    "&4\\u70B9<<<\\u335C"
    "&5\\u65E5<<<\\u33E4"
    "&5\\u6708<<<\\u32C4"
    "&5\\u70B9<<<\\u335D"
    "&6\\u65E5<<<\\u33E5"
    "&6\\u6708<<<\\u32C5"
    "&6\\u70B9<<<\\u335E"
    "&7\\u65E5<<<\\u33E6"
    "&7\\u6708<<<\\u32C6"
    "&7\\u70B9<<<\\u335F"
    "&8\\u65E5<<<\\u33E7"
    "&8\\u6708<<<\\u32C7"
    "&8\\u70B9<<<\\u3360"
    "&9\\u65E5<<<\\u33E8"
    "&9\\u6708<<<\\u32C8"
    "&9\\u70B9<<<\\u3361"
    "&\\u3014\\u4E09\\u3015<<<\\u01F241"
    "&\\u3014\\u4E8C\\u3015<<<\\u01F242"
    "&\\u3014\\u52DD\\u3015<<<\\u01F247"
    "&\\u3014\\u5B89\\u3015<<<\\u01F243"
    "&\\u3014\\u6253\\u3015<<<\\u01F245"
    "&\\u3014\\u6557\\u3015<<<\\u01F248"
    "&\\u3014\\u672C\\u3015<<<\\u01F240"
    "&\\u3014\\u70B9\\u3015<<<\\u01F244"
    "&\\u3014\\u76D7\\u3015<<<\\u01F246"
    "&\\u4E00<<<\\u2F00<<<\\u3192<<<\\u3280<<<\\u01F229"
    "&\\u4E01<<<\\u319C"
    "&\\u4E03<<<\\u3286"
    "&\\u4E09<<<\\u3194<<<\\u3282<<<\\u01F22A"
    "&\\u4E0A<<<\\u3196<<<\\u32A4"
    "&\\u4E0B<<<\\u3198<<<\\u32A6"
    "&\\u4E19<<<\\u319B"
    "&\\u4E28<<<\\u2F01"
    "&\\u4E2D<<<\\u3197<<<\\u32A5<<<\\u01F22D"
    "&\\u4E36<<<\\u2F02"
    "&\\u4E3F<<<\\u2F03"
    "&\\u4E59<<<\\u2F04<<<\\u319A"
    "&\\u4E5D<<<\\u3288"
    "&\\u4E85<<<\\u2F05"
    "&\\u4E8C<<<\\u2F06<<<\\u3193<<<\\u3281<<<\\u01F214"
    "&\\u4E94<<<\\u3284"
    "&\\u4EA0<<<\\u2F07"
    "&\\u4EA4<<<\\u01F218"
    "&\\u4EBA<<<\\u2F08<<<\\u319F"
    "&\\u4F01<<<\\u32AD"
    "&\\u4F11<<<\\u32A1"
    "&\\u512A<<<\\u329D"
    "&\\u513F<<<\\u2F09"
    "&\\u5165<<<\\u2F0A"
    "&\\u516B<<<\\u2F0B<<<\\u3287"
    "&\\u516D<<<\\u3285"
    "&\\u5182<<<\\u2F0C"
    "&\\u518D<<<\\u01F21E"
    "&\\u5196<<<\\u2F0D"
    "&\\u5199<<<\\u32A2"
    "&\\u51AB<<<\\u2F0E"
    "&\\u51E0<<<\\u2F0F"
    "&\\u51F5<<<\\u2F10"
    "&\\u5200<<<\\u2F11"
    "&\\u521D<<<\\u01F220"
    "&\\u524D<<<\\u01F21C"
    "&\\u5272<<<\\u01F239"
    "&\\u529B<<<\\u2F12"
    "&\\u52B4<<<\\u3298"
    "&\\u52F9<<<\\u2F13"
    "&\\u5315<<<\\u2F14"
    "&\\u531A<<<\\u2F15"
    "&\\u5338<<<\\u2F16<<<\\u32A9"
    "&\\u5341<<<\\u2F17<<<\\u3038<<<\\u3289"
    "&\\u5344<<<\\u3039"
    "&\\u5345<<<\\u303A"
    "&\\u5354<<<\\u32AF"
    "&\\u535C<<<\\u2F18"
    "&\\u5369<<<\\u2F19"
    "&\\u5370<<<\\u329E"
    "&\\u5382<<<\\u2F1A"
    "&\\u53B6<<<\\u2F1B"
    "&\\u53C8<<<\\u2F1C"
    "&\\u53CC<<<\\u01F212"
    "&\\u53E3<<<\\u2F1D"
    "&\\u53EF<<<\\u01F251"
    "&\\u53F3<<<\\u32A8<<<\\u01F22E"
    "&\\u5408<<<\\u01F234"
    "&\\u540D<<<\\u3294"
    "&\\u5439<<<\\u01F225"
    "&\\u554F<<<\\u3244"
    "&\\u55B6<<<\\u01F23A"
    "&\\u56D7<<<\\u2F1E"
    "&\\u56DB<<<\\u3195<<<\\u3283"
    "&\\u571F<<<\\u2F1F<<<\\u328F"
    "&\\u5730<<<\\u319E"
    "&\\u58EB<<<\\u2F20"
    "&\\u58F0<<<\\u01F224"
    "&\\u5902<<<\\u2F21"
    "&\\u590A<<<\\u2F22"
    "&\\u5915<<<\\u2F23"
    "&\\u591A<<<\\u01F215"
    "&\\u591C<<<\\u32B0"
    "&\\u5927<<<\\u2F24"
    "&\\u5927\\u6B63<<<\\u337D"
    "&\\u5929<<<\\u319D<<<\\u01F217"
    "&\\u5973<<<\\u2F25<<<\\u329B"
    "&\\u5B50<<<\\u2F26"
    "&\\u5B57<<<\\u01F211"
    "&\\u5B66<<<\\u32AB"
    "&\\u5B80<<<\\u2F27"
    "&\\u5B97<<<\\u32AA"
    "&\\u5BF8<<<\\u2F28"
    "&\\u5C0F<<<\\u2F29"
    "&\\u5C22<<<\\u2F2A"
    "&\\u5C38<<<\\u2F2B"
    "&\\u5C6E<<<\\u2F2C"
    "&\\u5C71<<<\\u2F2D"
    "&\\u5DDB<<<\\u2F2E"
    "&\\u5DE5<<<\\u2F2F"
    "&\\u5DE6<<<\\u32A7<<<\\u01F22C"
    "&\\u5DF1<<<\\u2F30"
    "&\\u5DFE<<<\\u2F31"
    "&\\u5E72<<<\\u2F32"
    "&\\u5E73\\u6210<<<\\u337B"
    "&\\u5E7A<<<\\u2F33"
    "&\\u5E7C<<<\\u3245"
    "&\\u5E7F<<<\\u2F34"
    "&\\u5EF4<<<\\u2F35"
    "&\\u5EFE<<<\\u2F36"
    "&\\u5F0B<<<\\u2F37"
    "&\\u5F13<<<\\u2F38"
    "&\\u5F50<<<\\u2F39"
    "&\\u5F61<<<\\u2F3A"
    "&\\u5F73<<<\\u2F3B"
    "&\\u5F8C<<<\\u01F21D"
    "&\\u5F97<<<\\u01F250"
    "&\\u5FC3<<<\\u2F3C"
    "&\\u6208<<<\\u2F3D"
    "&\\u6236<<<\\u2F3E"
    "&\\u624B<<<\\u2F3F<<<\\u01F210"
    "&\\u6253<<<\\u01F231"
    "&\\u6295<<<\\u01F227"
    "&\\u6307<<<\\u01F22F"
    "&\\u6355<<<\\u01F228"
    "&\\u652F<<<\\u2F40"
    "&\\u6534<<<\\u2F41"
    "&\\u6587<<<\\u2F42<<<\\u3246"
    "&\\u6597<<<\\u2F43"
    "&\\u6599<<<\\u01F21B"
    "&\\u65A4<<<\\u2F44"
    "&\\u65B0<<<\\u01F21F"
    "&\\u65B9<<<\\u2F45"
    "&\\u65E0<<<\\u2F46"
    "&\\u65E5<<<\\u2F47<<<\\u3290"
    "&\\u660E\\u6CBB<<<\\u337E"
    "&\\u6620<<<\\u01F219"
    "&\\u662D\\u548C<<<\\u337C"
    "&\\u66F0<<<\\u2F48"
    "&\\u6708<<<\\u2F49<<<\\u328A<<<\\u01F237"
    "&\\u6709<<<\\u3292<<<\\u01F236"
    "&\\u6728<<<\\u2F4A<<<\\u328D"
    "&\\u682A<<<\\u3291"
    "&\\u682A\\u5F0F\\u4F1A\\u793E<<<\\u337F"
    "&\\u6B20<<<\\u2F4B"
    "&\\u6B62<<<\\u2F4C"
    "&\\u6B63<<<\\u32A3"
    "&\\u6B79<<<\\u2F4D"
    "&\\u6BB3<<<\\u2F4E"
    "&\\u6BCB<<<\\u2F4F"
    "&\\u6BCD<<<\\u2E9F"
    "&\\u6BD4<<<\\u2F50"
    "&\\u6BDB<<<\\u2F51"
    "&\\u6C0F<<<\\u2F52"
    "&\\u6C14<<<\\u2F53"
    "&\\u6C34<<<\\u2F54<<<\\u328C"
    "&\\u6CE8<<<\\u329F"
    "&\\u6E80<<<\\u01F235"
    "&\\u6F14<<<\\u01F226"
    "&\\u706B<<<\\u2F55<<<\\u328B"
    "&\\u7121<<<\\u01F21A"
    "&\\u722A<<<\\u2F56"
    "&\\u7236<<<\\u2F57"
    "&\\u723B<<<\\u2F58"
    "&\\u723F<<<\\u2F59"
    "&\\u7247<<<\\u2F5A"
    "&\\u7259<<<\\u2F5B"
    "&\\u725B<<<\\u2F5C"
    "&\\u7279<<<\\u3295"
    "&\\u72AC<<<\\u2F5D"
    "&\\u7384<<<\\u2F5E"
    "&\\u7389<<<\\u2F5F"
    "&\\u74DC<<<\\u2F60"
    "&\\u74E6<<<\\u2F61"
    "&\\u7518<<<\\u2F62"
    "&\\u751F<<<\\u2F63<<<\\u01F222"
    "&\\u7528<<<\\u2F64"
    "&\\u7530<<<\\u2F65"
    "&\\u7532<<<\\u3199"
    "&\\u7533<<<\\u01F238"
    "&\\u7537<<<\\u329A"
    "&\\u758B<<<\\u2F66"
    "&\\u7592<<<\\u2F67"
    "&\\u7676<<<\\u2F68"
    "&\\u767D<<<\\u2F69"
    "&\\u76AE<<<\\u2F6A"
    "&\\u76BF<<<\\u2F6B"
    "&\\u76E3<<<\\u32AC"
    "&\\u76EE<<<\\u2F6C"
    "&\\u77DB<<<\\u2F6D"
    "&\\u77E2<<<\\u2F6E"
    "&\\u77F3<<<\\u2F6F"
    "&\\u793A<<<\\u2F70"
    "&\\u793E<<<\\u3293"
    "&\\u795D<<<\\u3297"
    "&\\u7981<<<\\u01F232"
    "&\\u79B8<<<\\u2F71"
    "&\\u79BE<<<\\u2F72"
    "&\\u79D8<<<\\u3299"
    "&\\u7A74<<<\\u2F73"
    "&\\u7A7A<<<\\u01F233"
    "&\\u7ACB<<<\\u2F74"
    "&\\u7AF9<<<\\u2F75"
    "&\\u7B8F<<<\\u3247"
    "&\\u7C73<<<\\u2F76"
    "&\\u7CF8<<<\\u2F77"
    "&\\u7D42<<<\\u01F221"
    "&\\u7F36<<<\\u2F78"
    "&\\u7F51<<<\\u2F79"
    "&\\u7F8A<<<\\u2F7A"
    "&\\u7FBD<<<\\u2F7B"
    "&\\u8001<<<\\u2F7C"
    "&\\u800C<<<\\u2F7D"
    "&\\u8012<<<\\u2F7E"
    "&\\u8033<<<\\u2F7F"
    "&\\u807F<<<\\u2F80"
    "&\\u8089<<<\\u2F81"
    "&\\u81E3<<<\\u2F82"
    "&\\u81EA<<<\\u2F83"
    "&\\u81F3<<<\\u2F84"
    "&\\u81FC<<<\\u2F85"
    "&\\u820C<<<\\u2F86"
    "&\\u821B<<<\\u2F87"
    "&\\u821F<<<\\u2F88"
    "&\\u826E<<<\\u2F89"
    "&\\u8272<<<\\u2F8A"
    "&\\u8278<<<\\u2F8B"
    "&\\u864D<<<\\u2F8C"
    "&\\u866B<<<\\u2F8D"
    "&\\u8840<<<\\u2F8E"
    "&\\u884C<<<\\u2F8F"
    "&\\u8863<<<\\u2F90"
    "&\\u897E<<<\\u2F91"
    "&\\u898B<<<\\u2F92"
    "&\\u89D2<<<\\u2F93"
    "&\\u89E3<<<\\u01F216"
    "&\\u8A00<<<\\u2F94"
    "&\\u8C37<<<\\u2F95"
    "&\\u8C46<<<\\u2F96"
    "&\\u8C55<<<\\u2F97"
    "&\\u8C78<<<\\u2F98"
    "&\\u8C9D<<<\\u2F99"
    "&\\u8CA1<<<\\u3296"
    "&\\u8CA9<<<\\u01F223"
    "&\\u8CC7<<<\\u32AE"
    "&\\u8D64<<<\\u2F9A"
    "&\\u8D70<<<\\u2F9B<<<\\u01F230"
    "&\\u8DB3<<<\\u2F9C"
    "&\\u8EAB<<<\\u2F9D"
    "&\\u8ECA<<<\\u2F9E"
    "&\\u8F9B<<<\\u2F9F"
    "&\\u8FB0<<<\\u2FA0"
    "&\\u8FB5<<<\\u2FA1"
    "&\\u904A<<<\\u01F22B"
    "&\\u9069<<<\\u329C"
    "&\\u9091<<<\\u2FA2"
    "&\\u9149<<<\\u2FA3"
    "&\\u914D<<<\\u01F23B"
    "&\\u91C6<<<\\u2FA4"
    "&\\u91CC<<<\\u2FA5"
    "&\\u91D1<<<\\u2FA6<<<\\u328E"
    "&\\u9577<<<\\u2FA7"
    "&\\u9580<<<\\u2FA8"
    "&\\u961C<<<\\u2FA9"
    "&\\u96B6<<<\\u2FAA"
    "&\\u96B9<<<\\u2FAB"
    "&\\u96E8<<<\\u2FAC"
    "&\\u9751<<<\\u2FAD"
    "&\\u975E<<<\\u2FAE"
    "&\\u9762<<<\\u2FAF"
    "&\\u9769<<<\\u2FB0"
    "&\\u97CB<<<\\u2FB1"
    "&\\u97ED<<<\\u2FB2"
    "&\\u97F3<<<\\u2FB3"
    "&\\u9801<<<\\u2FB4"
    "&\\u9805<<<\\u32A0"
    "&\\u98A8<<<\\u2FB5"
    "&\\u98DB<<<\\u2FB6"
    "&\\u98DF<<<\\u2FB7"
    "&\\u9996<<<\\u2FB8"
    "&\\u9999<<<\\u2FB9"
    "&\\u99AC<<<\\u2FBA"
    "&\\u9AA8<<<\\u2FBB"
    "&\\u9AD8<<<\\u2FBC"
    "&\\u9ADF<<<\\u2FBD"
    "&\\u9B25<<<\\u2FBE"
    "&\\u9B2F<<<\\u2FBF"
    "&\\u9B32<<<\\u2FC0"
    "&\\u9B3C<<<\\u2FC1"
    "&\\u9B5A<<<\\u2FC2"
    "&\\u9CE5<<<\\u2FC3"
    "&\\u9E75<<<\\u2FC4"
    "&\\u9E7F<<<\\u2FC5"
    "&\\u9EA5<<<\\u2FC6"
    "&\\u9EBB<<<\\u2FC7"
    "&\\u9EC3<<<\\u2FC8"
    "&\\u9ECD<<<\\u2FC9"
    "&\\u9ED1<<<\\u2FCA"
    "&\\u9EF9<<<\\u2FCB"
    "&\\u9EFD<<<\\u2FCC"
    "&\\u9F0E<<<\\u2FCD"
    "&\\u9F13<<<\\u2FCE"
    "&\\u9F20<<<\\u2FCF"
    "&\\u9F3B<<<\\u2FD0"
    "&\\u9F4A<<<\\u2FD1"
    "&\\u9F52<<<\\u2FD2"
    "&\\u9F8D<<<\\u2FD3"
    "&\\u9F9C<<<\\u2FD4"
    "&\\u9F9F<<<\\u2EF3"
    "&\\u9FA0<<<\\u2FD5"
    "&\\u02342F<\\u91CD\\u5E86/\\u5E86"
    "&\\u5F1E<\\u6C88\\u9633/\\u9633"
    "&\\u92BA<\\u85CF\\u6587/\\u6587";