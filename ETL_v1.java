import java.sql.*;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
public class ETL {
    private static final int TRANSACTION_STREAM_SIZE = 1000;
    private static final int TRANSACTION_DELAY_MS = 1000;
    private static final int MASTER_DATA_DELAY_MS = 1000;
    private static final int MASTER_DATA_STREAM_SIZE = 10;

    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/transactions";
        String username = "root";
        String password = "MySecurePwd123!";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(url, username, password);
            Statement transaction_statement = connection.createStatement();
            Statement master_data_statement = connection.createStatement();

            DoublyLinkedListQueue<Integer> productIdQueue = new DoublyLinkedListQueue<>();
            TransactionStream transactionStream = new TransactionStream(transaction_statement, productIdQueue);
            MasterDataStream masterDataStream = new MasterDataStream(master_data_statement);

            transactionStream.start();
            masterDataStream.start();

            HybridJoin monitoringThread = new HybridJoin(transactionStream, masterDataStream, transaction_statement, master_data_statement, productIdQueue);
            monitoringThread.start();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    static class TransactionStream extends Thread {
        private final Statement statement;
        private final DoublyLinkedListQueue<Integer> productIdQueue;
        private int transactionCount;

        TransactionStream(Statement statement, DoublyLinkedListQueue<Integer> productIdQueue) {
            this.statement = statement;
            this.productIdQueue = productIdQueue;
            this.transactionCount = 0;
        }

        public synchronized int getTransactionCount() {
            return transactionCount;
        }

        @Override
        public void run() {
            try {
                ResultSet resultSet = statement.executeQuery("SELECT Order_ID, Order_Date, ProductID, CustomerID, CustomerName, Gender, Quantity_Ordered FROM cleaned_transactions");

                while (resultSet.next()) {
                    int productID = resultSet.getInt("ProductID");
                    productIdQueue.enqueue(productID);
                    int orderID = resultSet.getInt("Order_ID");
                    LocalDateTime orderDate = resultSet.getObject("Order_Date", LocalDateTime.class);
                    int customerID = resultSet.getInt("CustomerID");
                    String customerName = resultSet.getString("CustomerName");
                    String gender = resultSet.getString("Gender");
                    int quantityOrdered = resultSet.getInt("Quantity_Ordered");

                /*    System.out.println("Order ID: " + orderID);
                    System.out.println("Order Date: " + orderDate);
                    System.out.println("Product ID: " + productID);
                    System.out.println("Customer ID: " + customerID);
                    System.out.println("Customer Name: " + customerName);
                    System.out.println("Gender: " + gender);
                    System.out.println("Quantity Ordered: " + quantityOrdered);*/

                    transactionCount++;
                    Thread.sleep(TRANSACTION_DELAY_MS / TRANSACTION_STREAM_SIZE);
                }
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    static class MasterDataStream extends Thread {
        private final Statement statement;
        private int masterDataCount;

        MasterDataStream(Statement statement) {
            this.statement = statement;
            this.masterDataCount = 0;
        }

        public synchronized int getMasterDataCount() {
            return masterDataCount;
        }

        @Override
        public void run() {
            try {
                ResultSet masterDataResultSet = statement.executeQuery("SELECT * FROM master_data_cleaned");

                while (masterDataResultSet.next()) {
                    int productID = masterDataResultSet.getInt("productID");
                    String productName = masterDataResultSet.getString("productName");
                    int productPrice = masterDataResultSet.getInt("productPrice");
                    int supplierID = masterDataResultSet.getInt("supplierID");
                    String supplierName = masterDataResultSet.getString("supplierName");
                    int storeID = masterDataResultSet.getInt("storeID");
                    String storeName = masterDataResultSet.getString("storeName");

                  /*  System.out.println("Master Data - Product ID: " + productID +
                            ", Product Name: " + productName +
                            ", Product Price: " + productPrice +
                            ", Supplier ID: " + supplierID +
                            ", Supplier Name: " + supplierName +
                            ", Store ID: " + storeID +
                            ", Store Name: " + storeName);*/

                    masterDataCount++;
                    Thread.sleep(MASTER_DATA_DELAY_MS / MASTER_DATA_STREAM_SIZE);
                }
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    static class HybridJoin extends Thread {
        private final TransactionStream transactionStream;
        private final MasterDataStream masterDataStream;
        private final Statement transaction_statement; // Include statement instances
        private final Statement master_data_statement;
        private final DoublyLinkedListQueue<Integer> productIdQueue; // Include productIdQueue


        HybridJoin(TransactionStream transactionStream, MasterDataStream masterDataStream,Statement transaction_statement,Statement master_data_statement, DoublyLinkedListQueue<Integer> productIdQueue) {
            this.transactionStream = transactionStream;
            this.masterDataStream = masterDataStream;
            this.transaction_statement = transaction_statement;
            this.master_data_statement = master_data_statement;
            this.productIdQueue = productIdQueue;
        }

        @Override
        public void run() {
            try {
                int transactionCount = 0;
                int masterDataCount = 0;
                while (true) {
                    int currentTransactionCount = transactionStream.getTransactionCount();
                    int currentMasterDataCount = masterDataStream.getMasterDataCount();

                    int transactionArrivalRate = currentTransactionCount - transactionCount;
                    int masterDataArrivalRate = currentMasterDataCount - masterDataCount;

                    transactionCount = currentTransactionCount;
                    masterDataCount = currentMasterDataCount;

                    System.out.println("Transaction Arrival Rate: " + transactionArrivalRate +
                            ", Master Data Arrival Rate: " + masterDataArrivalRate);

                    // Perform hybrid join when data is available from both streams
                    if (transactionArrivalRate > 0 && masterDataArrivalRate > 0) {
                        int joinedCount = performHybridJoin(transaction_statement, master_data_statement, productIdQueue);
                        System.out.println("Joined and inserted " + joinedCount + " records into OrderFacts.");
                    }

                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                System.out.println(e);
                Thread.currentThread().interrupt();
            }
            catch (SQLException e) {
                e.printStackTrace(); // Handle or log the exception accordingly
            }
        }

        // Function to perform hybrid join and insert into OrderFacts table
        private int performHybridJoin(Statement transaction_statement, Statement master_data_statement, DoublyLinkedListQueue<Integer> productIdQueue) throws SQLException {
            int joinedCount = 0;
            Connection dwConnection = DriverManager.getConnection("jdbc:mysql://localhost:3306/electronica_dw", "root", "MySecurePwd123!");
            PreparedStatement insertOrderFactsStatement = dwConnection.prepareStatement(
                    "INSERT INTO OrderFacts (OrderID, OrderDate, ProductID, CustomerID, QuantityOrdered, productPrice) " +
                            "VALUES (?, ?, ?, ?, ?, ?) " +
                            "ON DUPLICATE KEY UPDATE " +
                            "OrderID = VALUES(OrderID), " +
                            "OrderDate = VALUES(OrderDate), " +
                            "CustomerID = VALUES(CustomerID), " +
                            "QuantityOrdered = VALUES(QuantityOrdered), " +
                            "productPrice = VALUES(productPrice)");

            PreparedStatement insertSupplierStatement = dwConnection.prepareStatement(
                    "INSERT IGNORE INTO SupplierDimension (supplierID, supplierName) VALUES (?, ?)");

            PreparedStatement insertProductStatement = dwConnection.prepareStatement(
                    "INSERT IGNORE INTO ProductDimension (ProductID, productName, supplierID) VALUES (?, ?, ?)");

            PreparedStatement insertCustomerStatement = dwConnection.prepareStatement(
                    "INSERT IGNORE INTO CustomerDimension (CustomerID, CustomerName, Gender) VALUES (?, ?, ?)");

            PreparedStatement insertStoreStatement = dwConnection.prepareStatement(
                    "INSERT IGNORE INTO StoreDimension (storeID, storeName) VALUES (?, ?)");

            Set<Integer> uniqueProductIDs = new HashSet<>();

            try {
                while (!productIdQueue.isEmpty()) {
                    int productID = productIdQueue.dequeue();

                    if (!uniqueProductIDs.add(productID)) {
                        continue;
                    }

                    ResultSet combinedData = transaction_statement.executeQuery("SELECT ct.Order_ID, ct.Order_Date, ct.ProductID, ct.CustomerID, ct.Quantity_Ordered, mdc.productPrice, ct.CustomerName, ct.Gender, mdc.supplierID, mdc.productName, mdc.storeID, mdc.storeName " +
                            "FROM cleaned_transactions ct JOIN master_data_cleaned mdc ON ct.ProductID = mdc.productID " +
                            "WHERE ct.ProductID = " + productID);

                    while (combinedData.next()) {
                        int orderID = combinedData.getInt("Order_ID");
                        LocalDateTime orderDate = combinedData.getObject("Order_Date", LocalDateTime.class);
                        int fetchedProductID = combinedData.getInt("ProductID");
                        int customerID = combinedData.getInt("CustomerID");
                        int quantityOrdered = combinedData.getInt("Quantity_Ordered");
                        int productPrice = combinedData.getInt("productPrice");
                        String customerName = combinedData.getString("CustomerName");
                        String gender = combinedData.getString("Gender");
                        int supplierID = combinedData.getInt("supplierID");
                        String productName = combinedData.getString("productName");
                        int storeID = combinedData.getInt("storeID");
                        String storeName = combinedData.getString("storeName");

                        // Insert into OrderFacts table
                        insertOrderFactsStatement.setInt(1, orderID);
                        insertOrderFactsStatement.setObject(2, orderDate);
                        insertOrderFactsStatement.setInt(3, fetchedProductID);
                        insertOrderFactsStatement.setInt(4, customerID);
                        insertOrderFactsStatement.setInt(5, quantityOrdered);
                        insertOrderFactsStatement.setInt(6, productPrice);

                        int rowsAffected = insertOrderFactsStatement.executeUpdate();
                        if (rowsAffected > 0) {
                            joinedCount++;
                        }

                        // Insert into SupplierDimension table
                        insertSupplierStatement.setInt(1, supplierID);
                        insertSupplierStatement.setString(2, productName);
                        insertSupplierStatement.executeUpdate();

                        // Insert into ProductDimension table
                        insertProductStatement.setInt(1, fetchedProductID);
                        insertProductStatement.setString(2, productName);
                        insertProductStatement.setInt(3, supplierID);
                        insertProductStatement.executeUpdate();

                        // Insert into CustomerDimension table
                        insertCustomerStatement.setInt(1, customerID);
                        insertCustomerStatement.setString(2, customerName);
                        insertCustomerStatement.setString(3, gender);
                        insertCustomerStatement.executeUpdate();

                        // Insert into StoreDimension table
                        insertStoreStatement.setInt(1, storeID);
                        insertStoreStatement.setString(2, storeName);
                        insertStoreStatement.executeUpdate();
                    }
                    combinedData.close(); // Close the ResultSet after fully processing it for each productID
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                // Closing all prepared statements and connection

                // Closing insert statements
                if (insertOrderFactsStatement != null) {
                    insertOrderFactsStatement.close();
                }
                if (insertSupplierStatement != null) {
                    insertSupplierStatement.close();
                }
                if (insertProductStatement != null) {
                    insertProductStatement.close();
                }
                if (insertCustomerStatement != null) {
                    insertCustomerStatement.close();
                }
                if (insertStoreStatement != null) {
                    insertStoreStatement.close();
                }

                // Closing connection
                if (dwConnection != null) {
                    dwConnection.close();
                }
            }


            return joinedCount;
        }
    }
    static class DoublyLinkedListQueue<T> {
        private Node<T> front;
        private Node<T> rear;

        private static class Node<T> {
            T data;
            Node<T> prev;
            Node<T> next;

            Node(T data) {
                this.data = data;
                this.prev = null;
                this.next = null;
            }
        }

        public void enqueue(T data) {
            Node<T> newNode = new Node<>(data);
            if (rear == null) {
                front = newNode;
                rear = newNode;
            } else {
                rear.next = newNode;
                newNode.prev = rear;
                rear = newNode;
            }
        }

        public T dequeue() {
            if (front == null) {
                return null;
            }
            T data = front.data;
            if (front == rear) {
                front = null;
                rear = null;
            } else {
                front = front.next;
                front.prev = null;
            }
            return data;
        }
        public boolean isEmpty() {
            return front == null; // Returns true if front is null, indicating an empty queue.
        }

        public T peek() {
            return front != null ? front.data : null;
        }
    }
}


