package ACID_props;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BankTransaction {
  public static final String URL = "jdbc:postgresql://localhost:5432/database_consistency";
  private static final  String USER = "safwankherallah";
  private static final String PASSWORD = "123456";

  public static void transferMoney(String sourceAccountNumber,
      String destinationAccountNumber,
      BigDecimal amount) throws SQLException {
    Connection conn = null;
    try{
      conn = DriverManager.getConnection(URL, USER, PASSWORD);
      conn.setAutoCommit(false); // Start transaction

      // Step 1 : Lock and check balance of source account
      PreparedStatement checkBalanceStmt = conn.prepareStatement(
          "SELECT balance FROM accounts WHERE account_number = ? FOR UPDATE"
      );

      checkBalanceStmt.setString(1, sourceAccountNumber);

      ResultSet rs = checkBalanceStmt.executeQuery();

      if(!rs.next() || rs.getBigDecimal("balance").compareTo(amount) < 0 ){
        conn.rollback();
        throw  new RuntimeException("Insufficient funds");
      }

      // Step 2: Deduct amount from source account

      PreparedStatement deductStmt = conn.prepareStatement(
          "UPDATE  accounts SET balance = balance - ? WHERE account_number = ?"
      );

      deductStmt.setBigDecimal(1, amount);
      deductStmt.setString(2, sourceAccountNumber);
      deductStmt.executeUpdate();

      // Step 3: ADD the amount to the destination account
      PreparedStatement addStmt = conn.prepareStatement(
          "UPDATE  accounts SET balance = balance + ? WHERE account_number = ? "
      );

      addStmt.setBigDecimal(1, amount);
      addStmt.setString(2, destinationAccountNumber);
      addStmt.executeUpdate();

      conn.commit(); // Commit the transaction

    } catch (SQLException e) {
      if(conn != null){
        conn.rollback(); // Roll back in case of error
      }
      throw e;
    }finally {
      if(conn != null){
        conn.setAutoCommit(true); // Reset  Auto Commit
        conn.close();
      }
    }

  }

  public static void main(String[] args) {
    try {
      transferMoney("543216789", "678954321", BigDecimal.valueOf(10));
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }


  // Step methods:
  // check for capacity : boolean


}
