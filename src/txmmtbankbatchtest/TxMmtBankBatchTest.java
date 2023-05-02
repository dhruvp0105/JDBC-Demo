package txmmtbankbatchtest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class TxMmtBankBatchTest {

    public static void main(String[] args) throws ClassNotFoundException {

        Scanner sc = new Scanner(System.in);
        System.out.println("Please Enter the Source Account No : ");
        int srcno = sc.nextInt();

        System.out.println("Please Enter the Destination Account No : ");
        int destno = sc.nextInt();

        System.out.println("Please Enter the Amount to transfer : ");
        int amt = sc.nextInt();

        try {
            Class.forName("org.apache.derby.jdbc.ClientDriver");
            System.out.println("Hello");
            Connection con = DriverManager.getConnection("jdbc:derby://localhost:1527/TransactionMmtBank", "dhruv", "dhruv");
            Statement st = con.createStatement();

            con.setAutoCommit(false);

            //withdraw amount
            st.addBatch("update DHRUV.ACCOUNT set balance=balance-" + amt + "where acc_id=" + srcno);

            //deposit amount
            st.addBatch("update DHRUV.ACCOUNT set balance=balance+" + amt + "where acc_id=" + destno);

            int res[] = st.executeBatch();

            //perform transaction management
            boolean flag = false;
            for (int i = 0; i < res.length; i++) {
                System.out.println("res" + i + "is" + res[i]);
                if (res[i] == 0) {
                    flag = true;
                    break;
                }
            }
            if (flag) {
                con.rollback(); //transaction is not completed successfully...
                System.out.println("Transaction is Rollback, Amount is not Transfer Successfully");

            } else {
                con.commit(); //transaction is completed successfully...
                System.out.println("Transaction is Commited, Amount Transfer Successfully");
            }

            String sql = "select * from DHRUV.ACCOUNT";
            Statement st1 = con.createStatement();
            ResultSet rs = st1.executeQuery(sql);

            ResultSetMetaData rsmd = rs.getMetaData();

            int columnCount = rsmd.getColumnCount();
            System.out.println("Total No of Column : " + columnCount);

            for (int i = 1; i <= columnCount; i++) {
                String columnName = rsmd.getColumnName(i);
                String columnType = rsmd.getColumnTypeName(i);
                System.out.println("Column Name " + columnName + " And Column Type : " + columnType);
            }
            rs.close();
            st.close();
            con.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

    }

}
