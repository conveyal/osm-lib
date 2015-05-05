package com.conveyal.osmlib.display;

import com.conveyal.osmlib.OSMEntitySink;
import com.conveyal.osmlib.OSMEntitySource;
import com.conveyal.osmlib.VexInput;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseWheelEvent;
import java.awt.event.MouseWheelListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.geom.AffineTransform;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.net.URL;

class Surface extends JPanel implements ActionListener, MouseListener, MouseWheelListener {


    final double METERS_PER_DEGREE_LAT = 111111.0;
    double metersPerPixel = 1000.0;
    double pixelsPerDegreeLat; // updated from the previous two fields

    Rectangle2D wgsWindow;

    private final int DELAY = 20;
    private Timer timer;

    // Drag
    int lastX = -1;
    int lastY = -1;

    public Surface() {
        initTimer();
        this.addMouseListener(this);
        this.addMouseWheelListener(this);
        wgsWindow = new Rectangle.Double(-73.8,40.61,0.05, 0.05);
    }

    private void initTimer() {
        timer = new Timer(DELAY, this);
        //timer.start();
    }

    public Timer getTimer() {
        return timer;
    }

    private static final String format = "http://a.tile.thunderforest.com/transport/%d/%d/%d.png";

    // cacheloader for tiles?

    public void drawMapTiles (Graphics2D g2d) {
        int minX = WebMercatorTile.xTile(wgsWindow.getMinX(), 11);
        int maxY = WebMercatorTile.yTile(wgsWindow.getMinY(), 11);
        int maxX = WebMercatorTile.xTile(wgsWindow.getMaxX(), 11);
        int minY = WebMercatorTile.yTile(wgsWindow.getMaxY(), 11); // min y is from max coord.
        for (int y = minY; y <= maxY; y++) {
            for (int x = minX; x <= maxX; x++) {
                Rectangle2D box = WebMercatorTile.getRectangle(x, y, 11);
                try {
                    URL url = new URL(String.format(format, 11, x, y));
                    BufferedImage image = ImageIO.read(url);
                    AffineTransform transform =
                            AffineTransform.getTranslateInstance(box.getMinX(), box.getMinY());
                    transform.getScaleInstance(box.getWidth() / 256, box.getHeight() / -256);
                    g2d.drawImage(image, transform, null);
//                    g2d.drawImage(image, 0, 0, null);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                break;
            }
        }
    }

    /** Very crude -- just fetch the entire window from VEX and draw it. */
    public void drawOsm (Graphics2D g2d) {
        try {
            String urlString = String.format("http://corneuve:9001/%f,%f,%f,%f",
                    wgsWindow.getMinY(),wgsWindow.getMinX(),wgsWindow.getMaxY(),wgsWindow.getMaxX());
            URL url = new URL(urlString);
            InputStream vexStream = url.openStream();
            OSMEntitySink graphicsSink = new GraphicsSink(g2d);
            OSMEntitySource vexSource = new VexInput(vexStream, graphicsSink);
            vexSource.read();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // TODO async, outside paint function.
    private void doDrawing(Graphics g) {
        Graphics2D g2d = (Graphics2D) g;
        AffineTransform savedTransform = g2d.getTransform();
        // Apply transforms on top of screen coords. Last specified => first applied.
        pixelsPerDegreeLat = METERS_PER_DEGREE_LAT / metersPerPixel;
        g2d.scale(pixelsPerDegreeLat * Math.cos(Math.toRadians(wgsWindow.getCenterY())), -pixelsPerDegreeLat);
        g2d.translate(-wgsWindow.getMinX(), -wgsWindow.getMinY());
        //g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        //g2d.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        g2d.setPaint(Color.blue);
        g2d.setStroke(new BasicStroke(0.0001f));
        //g2d.setStroke(new BasicStroke(2));
        // drawMapTiles(g2d);
        drawOsm(g2d);
        g2d.setTransform(savedTransform);
    }

    @Override
    public void paintComponent(Graphics g) {
        super.paintComponent(g);
        doDrawing(g);
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        repaint();
    }

    @Override
    public void mouseClicked(MouseEvent e) {
    }

    @Override
    public void mousePressed(MouseEvent e) {
        lastX = e.getX();
        lastY = e.getY();
    }

    @Override
    public void mouseReleased(MouseEvent e) {
        int dx = e.getX() - lastX;
        int dy = e.getY() - lastY;
        double newLon = wgsWindow.getMinX() - dx / pixelsPerDegreeLat;
        double newLat = wgsWindow.getMinY() + dy / pixelsPerDegreeLat;
        wgsWindow.setRect(newLon, newLat, 0.01, 0.01);
        repaint();
    }

    @Override
    public void mouseEntered(MouseEvent e) {

    }

    @Override
    public void mouseExited(MouseEvent e) {

    }

    @Override
    public void mouseWheelMoved(MouseWheelEvent e) {

        int x = e.getX();
        int y = e.getY();

        metersPerPixel *= (1 + (0.1 * e.getWheelRotation()));
        repaint();
    }
}

public class Display extends JFrame {

    public Display() {
        initUI();
    }

    private void initUI() {
        final Surface surface = new Surface();
        this.add(surface);
        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                Timer timer = surface.getTimer();
                timer.stop();
            }
        });
        setTitle("Points");
        setSize(350, 250);
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    public static void main(String[] args) {
        EventQueue.invokeLater(new Runnable() {
            @Override
            public void run() {
                Display ex = new Display();
                ex.setVisible(true);
            }
        });
    }

}
