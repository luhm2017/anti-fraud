package com.lakala.finance.anti_fraud.common;

/**
 * Created by longxiaolei on 2017/7/4.
 */
public interface SQL {
    interface NEO4J {
        //        String DEGREE1 = "Match (v1:ApplyInfo {orderno:'${orderno}'})-[e1:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]->(v2)<-[e2:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]-(v3:ApplyInfo) return v1.orderno,e1,v2.content,e2,v3.orderno";
        //        String DEGREE2 = "MATCH (v1:ApplyInfo {orderno:'${orderno}'})-[e1:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]->(v2)<-[e2:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]-(v3:ApplyInfo)-[e3:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]->(v4)<-[e4:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]-(v5:ApplyInfo)  RETURN v1.orderno, e1, v2.content, e2, v3.orderno, e3, v4.content, e4, v5.orderno";
        String IS_WITE = "match (v1:ApplyInfo {orderno:'${orderno}'}) -[e1:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]-> (v2 {type:'3'}) return v1 limit 1";
        String DEGREE1_PAGE = "Match (v1:ApplyInfo {orderno:'${orderno}'})-[e1:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]->(v2)<-[e2:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]-(v3:ApplyInfo) return v1.orderno,e1,v2.content,e2,v3.orderno skip ? limit ?";
        String DEGREE2_PAGE = "MATCH (v1:ApplyInfo {orderno:'${orderno}'})-[e1:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]->(v2)<-[e2:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]-(v3:ApplyInfo)-[e3:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]->(v4)<-[e4:emergencymobile|applymymobile|loanapply|recommend|relativemobile|hometel|loginmobile|device|bankcard|email|identification]-(v5:ApplyInfo)  RETURN v1.orderno, e1, v2.content, e2, v3.orderno, e3, v4.content, e4, v5.orderno skip ? limit ?";
    }

    interface MYSQL {
        String INSET_EXCEPTION = "INSERT INTO EXCEPTION_ORDERNO_TBL (ORDERNO,INSERT_TIME,EX_TYPE) VALUES(?,?,?)";
        String INSET_EXCEPTION_2VAR = "INSERT INTO EXCEPTION_ORDERNO_TBL2VAR (ORDERNO,INSERT_TIME,EX_TYPE) VALUES(?,?,?)";
    }
}
