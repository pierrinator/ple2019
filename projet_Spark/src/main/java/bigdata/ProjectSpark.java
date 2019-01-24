package bigdata;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

public class ProjectSpark {

	private static int row;
	private static final byte[] XFAMILY = Bytes.toBytes("x");
    private static final byte[] YFAMILY = Bytes.toBytes("y");
    private static final byte[] ZFAMILY = Bytes.toBytes("z");
    private static final byte[] PICTUREFAMILY = Bytes.toBytes("picture");
    private static final byte[] TABLE_NAME = Bytes.toBytes("mduverneix_tiles");

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createTable(Connection connect) {
        try {
            Admin admin = connect.getAdmin();
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            HColumnDescriptor famX = new HColumnDescriptor(XFAMILY);
            HColumnDescriptor famY = new HColumnDescriptor(YFAMILY);
            HColumnDescriptor famZ = new HColumnDescriptor(ZFAMILY);
            HColumnDescriptor famPicture = new HColumnDescriptor(PICTUREFAMILY);

            tableDescriptor.addFamily(famX);
            tableDescriptor.addFamily(famY);
            tableDescriptor.addFamily(famZ);
            tableDescriptor.addFamily(famPicture);
            createOrOverwrite(admin, tableDescriptor);
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void addToHBase(Table table, int x, int y, int z, BufferedImage image) {
        Put put = new Put(Bytes.toBytes("row" + Integer.toString(row)));
        put.addColumn(Bytes.toBytes("x"), Bytes.toBytes("x"), Bytes.toBytes(x));
        put.addColumn(Bytes.toBytes("y"), Bytes.toBytes("y"), Bytes.toBytes(y));
		put.addColumn(Bytes.toBytes("z"), Bytes.toBytes("z"), Bytes.toBytes(z));

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			ImageIO.write(image, "jpg", bos);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		byte[] imageInByte = bos.toByteArray();
		
		put.addColumn(Bytes.toBytes("picture"), Bytes.toBytes("picture"), imageInByte);
        try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
		row++;
	}
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Project Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		Picture picture = new Picture();
		row = 1;

		Connection connection;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection();
			createTable(connection);
			table = connection.getTable(TableName.valueOf(TABLE_NAME));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

        String path = args[0];
		JavaPairRDD<String, PortableDataStream> lines = context.binaryFiles(path);
		
		JavaRDD<PortableDataStream> res = lines.map(x -> x._2);
		PortableDataStream element = res.first();

		List<Integer> heights = picture.getHeights(element);

		BufferedImage image = picture.createPicture(heights, 1201, 1201);

		int pictureX = picture.getX(path);
		int pictureY = picture.getY(path);
		
		addToHBase(table, pictureX, pictureY, 0, image);
		
		File outputFile = new File(args[1]);

		if(!outputFile.exists()){
			try {
				outputFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		  }else{
			System.out.println("File already exists");
		  }

		try {
			ImageIO.write(image, "png", outputFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
       
	}
	
}
