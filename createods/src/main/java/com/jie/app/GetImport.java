package com.jie.app;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.jie.utils.DruidUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class GetImport {
    public static void main(String[] args) throws SQLException {
        //import_data /opt/module/datax/job/import/gmall.activity_info.json /origin_data/gmall/db/activity_info_full/$do_date
        DruidDataSource ds = DruidUtils.getDruidSource("10.0.0.15",3306,"root","123456");
        //1. 获取连接
        DruidPooledConnection connection = ds.getConnection();
        Statement st = connection.createStatement();
			/*
			int did = rs.getInt("did");//也可以根据列名称，并且可以按照数据类型获取
			String dname = rs.getString("dname");
			String desc = rs.getString("description");
			 */

            //System.out.println(did + "\t" );
        //2.获取所需表
        List<String> tables = new ArrayList<>();
        List<String> deserveTables = new ArrayList<>();

        ResultSet rs = st.executeQuery("SELECT DISTINCT TABLE_NAME from information_schema.COLUMNS WHERE TABLE_SCHEMA='jie_mdm' ORDER BY TABLE_NAME");
            while(rs.next()) {
                tables.add(rs.getString(1));
			/*
			int did = rs.getInt("did");//也可以根据列名称，并且可以按照数据类型获取
			String dname = rs.getString("dname");
			String desc = rs.getString("description");
			 */
            }
        //3.填入不需要的表
        deserveTables.add("ods_srm_supplychain_plan_delivery");
        deserveTables.add("ods_supplychain_plan_production_line_design_capacity");
        deserveTables.add("ods_excel_jrt_module_bom_all");
        deserveTables.add("ods_management_field_clock");
        deserveTables.add("ods_oa_ecl_bpm_flow_history");
        deserveTables.add("ods_oa_ecl_bpm_formdata_str");
        deserveTables.add("ods_oa_ecl_bpm_formdata_time");
        deserveTables.add("ods_sap_supplychain_plan_manufacture_order_details_copy1");
        deserveTables.add("ods_sap_marketing_sale_performance_report");
        deserveTables.add("ods_sap_marketing_sale_credential");
        deserveTables.add("ods_wecom_management_user_message");
        deserveTables.add("ods_wecom_marketing_service_order");
        deserveTables.add("ods_dahua_management_attendance_record");
        deserveTables.add("wecome_management_field_clock");


        //System.out.println(databases);
        //System.out.println(tables_databases);

        //4. 生成命令
        //eg : import_data /opt/module/datax/datax/job/import/linkbase.api_inter_info.json /origin_data/jie/api_inter_info/$do_date
        LinkedHashSet<String> commands = new LinkedHashSet<>();

        for (String table : tables) {
//            System.out.println(table);
            if (table.startsWith("ods_yunbiao2") && ! table.startsWith("ods_static") && ! deserveTables.contains(table)){
//                if(table.startsWith("ods_static")){
                String command1 = "echo \"=============" + table + "开始上传hdfs===============\"";
                commands.add(command1);

                String command2 = "import_data $DATAX_HOME/job/" + table.substring(4) + " "
                        + " /origin_data/jie/"+ table.substring(4) + "/$do_date";
                commands.add(command2);
//
//                String command3 = "test " + "/origin_data/jie/"+ table.substring(4) + "/$do_date";
//                commands.add(command3);

//                String command4 = "select *  from " + table + " where dt = \'9999-12-31\' limit 1;";
//                commands.add(command4);

//                String table1 = table.substring(4);
//                String words = " \""+ table1 +"\" )\n" +
//                        "        import_data $DATAX_HOME/job/"+ table1 + "  /origin_data/jie/"+ table1 + "/$do_date" + '\n' +
//                        " ;;" + "\n";
//                commands.add(words);

//                 String command5 = "    \"" + table + "\")\n" +
//                         "    load_data \""+ table +"\"\n" +
//                         "    ;;";
//                 commands.add(command5);
            }

        }


        int tableNum = 0;
        for (String command : commands) {
            System.out.println(command);
            tableNum++;
        }
        System.out.println();
        System.out.println("有效表总共有" + tableNum);
    }
}
