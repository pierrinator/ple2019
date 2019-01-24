package bigdata;

import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.input.PortableDataStream;

public class Picture {

    public List<Integer> getHeights(PortableDataStream element) {
		byte[] pixels = element.toArray();
		List<Integer> heights = new ArrayList<Integer>();

		for(int i=1; i<pixels.length; i+=2) {
			int height = (pixels[i-1] << 8) | pixels[i];  
			heights.add(height);
        }
        return heights;
    }

    public BufferedImage createPicture(List<Integer> heights, int width, int height) {
        int type = BufferedImage.TYPE_INT_RGB;
        BufferedImage picture = new BufferedImage(width, height, type);

        List<Integer> listheight = heights;

        int cpt = 0;
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
               int rgb = 0;
               
               if(listheight.get(cpt) <= 0){
                    rgb = 0;
                    rgb = (rgb<<8) + 128;
                    rgb = (rgb << 8) + 255;
                }
                if(listheight.get(cpt) > 0 && listheight.get(cpt) <= 100) {
                    rgb = 0;
                    rgb = (rgb<<8) + 255;
                    rgb = (rgb << 8) + listheight.get(cpt);
                }
                if(listheight.get(cpt) > 100 && listheight.get(cpt) < 200) {
                    rgb = 255;
                    rgb = (rgb<<8) + (300-listheight.get(cpt));
                    rgb = (rgb << 8) + 0;
                }
                if (listheight.get(cpt) > 200){
                    rgb = listheight.get(cpt);
                    rgb = (rgb << 8) + listheight.get(cpt);
                    rgb = (rgb << 8) + listheight.get(cpt);
                }
                

               picture.setRGB(x, y, rgb);
               cpt++;
            }
         }

        return picture;
    }

    public int getX(String path) {
        String tokens[] = path.split("/");
        String filename = tokens[4];
        String strlongitude = filename.substring(1, 3);
        String strlatitude = filename.substring(4, 7);
        double longitude = Double.parseDouble(strlongitude);
        double latitude = Double.parseDouble(strlatitude);

        longitude = Math.toRadians(longitude);
        latitude = Math.toRadians(latitude);

        double x = 6371 * Math.cos(latitude) * Math.cos(longitude);
        return (int) x;
    }

    public int getY(String path) {
        String tokens[] = path.split("/");
        String filename = tokens[4];
        String strlongitude = filename.substring(1, 3);
        String strlatitude = filename.substring(4, 7);
        double longitude = Double.parseDouble(strlongitude);
        double latitude = Double.parseDouble(strlatitude);

        longitude = Math.toRadians(longitude);
        latitude = Math.toRadians(latitude);

        double y = 6371 * Math.cos(latitude) * Math.sin(longitude);
        return (int) y;
    }



}