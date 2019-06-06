import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Scanner;

public class Populate {
    static Connection connection;   //global connection object
    public static void main(String[] args) {

        DB_Helper db = new DB_Helper();
        try
        {
            //get connection into global object
            connection = db.getConnection();
            //empty the tables and insert data
            emptyTable("MOVIES");
            insertMovieData();
            emptyTable("TAGS");
            insertTagData();
            emptyTable("MOVIE_TAGS");
            insertMovieTagData();
            emptyTable("MOVIE_COUNTRIES");
            insertMovieCountriesData();
            emptyTable("MOVIE_GENRE");
            insertMovieGenreData();
            emptyTable("MOVIE_LOCATIONS");
            insertMovieLocationData();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally {
            db.destroyConnection(connection);
        }
    }

    private static void emptyTable(String tableName)
    {
        try {
            //Connection connection = db.getConnection();
            String deleteQuery = "DELETE FROM "+tableName;
            PreparedStatement delete = connection.prepareStatement(deleteQuery);
            delete.executeUpdate();
            System.out.println("Table name = "+tableName+" deleted...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertMovieData()
    {
        try {
            //Connection connection = db.getConnection();
            System.out.println("Inserting into MOVIES");
            String insertQuery = "INSERT INTO MOVIES VALUES(?,?,?,?,?,?,?,?,?)";
            PreparedStatement insert = connection.prepareStatement(insertQuery);
            Scanner sc = new Scanner((new FileInputStream("D:\\University Study\\Database COEN280\\hetrec2011-movielens-2k-v2\\movies.dat")), "UTF-8");
            sc.nextLine();
            //int c = 0;
            while (sc.hasNextLine()) {
                //System.out.println(c);
                //c++;
                String line = sc.nextLine();
                String[] column = line.split("\t");
                insert.setString(1, column[0]);
                insert.setString(2,column[1]);
                insert.setInt(3, Integer.parseInt(column[5]));
                if (column[7] != null && !column[7].equals(" ") && !column[7].equals("\\N")) {
                    insert.setDouble(4, Double.parseDouble(column[7]));
                }
                if (column[8] != null && !column[8].equals(" ") && !column[8].equals("\\N")) {
                    insert.setInt(5, Integer.parseInt(column[8]));
                }
                if (column[12] != null && !column[12].equals(" ") && !column[12].equals("\\N")) {
                    insert.setDouble(6, Double.parseDouble(column[12]));
                }
                if (column[13] != null && !column[13].equals(" ") && !column[13].equals("\\N")) {
                    insert.setInt(7, Integer.parseInt(column[13]));
                }
                if (column[17] != null && !column[17].equals(" ") && !column[17].equals("\\N")) {
                    insert.setDouble(8, Double.parseDouble(column[17]));
                }
                if (column[18] != null && !column[18].equals(" ") && !column[18].equals("\\N")) {
                    insert.setInt(9, Integer.parseInt(column[18]));
                }
                insert.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertTagData()
    {
        try {
            //Connection connection = db.getConnection();
            System.out.println("Inserting into TAGS");
            String insertQuery = "INSERT INTO TAGS VALUES(?,?)";
            PreparedStatement insert = connection.prepareStatement(insertQuery);
            Scanner sc = new Scanner((new FileInputStream("D:\\University Study\\Database COEN280\\hetrec2011-movielens-2k-v2\\tags.dat")), "UTF-8");
            sc.nextLine();
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] column = line.split("\t");
                insert.setString(1, column[0]);
                insert.setString(2,column[1]);
                insert.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertMovieTagData()
    {
        try {
            System.out.println("Inserting into MOVIE_TAGS");
            String insertQuery = "INSERT INTO MOVIE_TAGS VALUES(?,?,?)";
            PreparedStatement insert = connection.prepareStatement(insertQuery);
            Scanner sc = new Scanner((new FileInputStream("D:\\University Study\\Database COEN280\\hetrec2011-movielens-2k-v2\\movie_tags.dat")), "UTF-8");
            sc.nextLine();
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] column = line.split("\t");
                insert.setString(1, column[0]);
                insert.setString(2,column[1]);
                insert.setDouble(3,Double.parseDouble(column[2]));
                insert.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertMovieCountriesData()
    {
        try {
            System.out.println("Inserting into MOVIE_COUNTRIES");
            String insertQuery = "INSERT INTO MOVIE_COUNTRIES VALUES(?,?)";
            PreparedStatement insert = connection.prepareStatement(insertQuery);
            Scanner sc = new Scanner((new FileInputStream("D:\\University Study\\Database COEN280\\hetrec2011-movielens-2k-v2\\movie_countries.dat")), "UTF-8");
            sc.nextLine();
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] column = line.split("\t");
                if (column.length < 2) {
                    continue;
                }
                insert.setString(1, column[0]);
                insert.setString(2,column[1]);
                insert.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertMovieGenreData()
    {
        try {
            System.out.println("Inserting into MOVIE_GENRE");
            String insertQuery = "INSERT INTO MOVIE_GENRE VALUES(?,?)";
            PreparedStatement insert = connection.prepareStatement(insertQuery);
            Scanner sc = new Scanner((new FileInputStream("D:\\University Study\\Database COEN280\\hetrec2011-movielens-2k-v2\\movie_genres.dat")), "UTF-8");
            sc.nextLine();
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] column = line.split("\t");
                insert.setString(1, column[0]);
                insert.setString(2,column[1]);
                insert.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertMovieLocationData()
    {
        try {
            System.out.println("Inserting into MOVIE_LOCATIONS");
            String insertQuery = "INSERT INTO MOVIE_LOCATIONS VALUES(?,?,?,?,?)";
            PreparedStatement insert = connection.prepareStatement(insertQuery);
            Scanner sc = new Scanner((new FileInputStream("D:\\University Study\\Database COEN280\\hetrec2011-movielens-2k-v2\\movie_locations.dat")), "UTF-8");
            sc.nextLine();
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] column = line.split("\t");
                if (column.length < 2) {
                    continue;
                }
                for(int c = 0;c<5;c++)
                {
                    try
                    {
                        //String m = column[c];
                        if (column[c] != null && !column[c].equals(" ") && !column[c].equals("\\N"))
                        {
                            insert.setString(c+1,column[c]);
                        }
                    }
                    catch (ArrayIndexOutOfBoundsException e)
                    {
                        insert.setString(c+1,null);
                    }
                }
                insert.executeUpdate();


            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
