import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import UI.UITextCell;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.scene.text.Font;
import javafx.stage.Stage;
import javafx.util.Callback;
import scala.Tuple2;
import utils.Utilities;

public class StreetStage {
    public static final Map<String, Double> category = new HashMap<String, Double>();

	public static final Map<Double, String> categoryInversed;
    public static final Prediction prediction = new Prediction();

    static {
        category.put("SEX OFFENSES, NON FORCIBLE", 35.0);
        category.put("BURGLARY", 8.0);
        category.put("KIDNAPPING", 24.0);
        category.put("LARCENY/THEFT", 0.0);
        category.put("STOLEN PROPERTY", 18.0);
        category.put("NON-CRIMINAL", 2.0);
        category.put("VEHICLE THEFT", 4.0);
        category.put("TRESPASS", 16.0);
        category.put("PORNOGRAPHY/OBSCENE MAT", 37.0);
        category.put("DRUG/NARCOTIC", 5.0);
        category.put("VANDALISM", 6.0);
        category.put("RECOVERED VEHICLE", 22.0);
        category.put("RUNAWAY", 25.0);
        category.put("DISORDERLY CONDUCT", 20.0);
        category.put("BAD CHECKS", 32.0);
        category.put("EMBEZZLEMENT", 28.0);
        category.put("PROSTITUTION", 17.0);
        category.put("SEX OFFENSES, FORCIBLE", 19.0);
        category.put("ROBBERY", 11.0);
        category.put("SUSPICIOUS OCC", 9.0);
        category.put("LOITERING", 29.0);
        category.put("WEAPON LAWS", 15.0);
        category.put("TREA", 38.0);
        category.put("EXTORTION", 34.0);
        category.put("DRUNKENNESS", 21.0);
        category.put("ASSAULT", 3.0);
        category.put("BRIBERY", 33.0);
        category.put("GAMBLING", 36.0);
        category.put("FORGERY/COUNTERFEITING", 14.0);
        category.put("ARSON", 27.0);
        category.put("WARRANTS", 7.0);
        category.put("DRIVING UNDER THE INFLUENCE", 23.0);
        category.put("OTHER OFFENSES", 1.0);
        category.put("MISSING PERSON", 10.0);
        category.put("FAMILY OFFENSES", 31.0);
        category.put("SECONDARY CODES", 13.0);
        category.put("LIQUOR LAWS", 26.0);
        category.put("SUICIDE", 30.0);
        category.put("FRAUD", 12.0);

        categoryInversed = MapUtils.invertMap(category);
    }

    public void display(

            XYChart.Series series,			
			final JavaRDD<Row> incidentDataRowRDD,
			final SparkSession spark,
			final StructType schemaRF,
			final PipelineModel modelRF,
			final StructType schemaNB,
			final PipelineModel modelNB,
			final SQLContext sqlContext

    ) {

		Stage subStage = new Stage();
		FlowPane root = new FlowPane();
		Scene scene = new Scene(root, 300, 200);

		ListView<String> list = new ListView<String>();
		ObservableList<String> data = FXCollections.observableArrayList();

		Label label = new Label();

 
        subStage.setTitle("Street");

        Map<String, Integer> streetHashMap = new HashMap<String, Integer>();

        JavaRDD<String> streetsRDD = incidentDataRowRDD.map(row -> row.getString(8).replaceAll("Block of ", "")
                .replaceAll("[0-9]+\\s", "").replaceAll("^\\s+", "") + " "
                + new Point(row.getDouble(9), row.getDouble(10)));

        JavaPairRDD<String, Integer> pairs = streetsRDD.mapToPair(s -> new Tuple2(s, 1));
        List<Tuple2<String, Integer>> counts = pairs.reduceByKey((a, b) -> a + b).sortByKey().collect();

        for (Tuple2<String, Integer> street : counts) {
            if (street._1.contains("/")) {
                data.add(street._1);
            }

            streetHashMap.put(street._1.toLowerCase(), street._2.intValue());
        }

        JavaRDD<Row> streetRDD = pairs.reduceByKey((a, b) -> a + b).map((t) -> {
            return RowFactory.create(t._1, t._2.intValue() > 2000 ? 2000 : t._2.intValue());
        });

        List<String> tableColumns1 = Arrays.asList("street", "count");

        StructType schema1 = Utilities.createSchema(tableColumns1);
        Dataset<Row> rddFilteredData = spark.createDataFrame(streetRDD.rdd(), schema1);

        List<Row> rows = rddFilteredData.describe("count").collectAsList();

        Double count = 0.0;
        Double mean = 0.0;
        Double stddev = 0.0;
        Double min = 0.0;
        Double max = 0.0;

        for (Row row : rows) {
            if (row.get(0).equals("count")) {
                count = Double.valueOf(row.get(1).toString());
            }
            if (row.get(0).equals("mean")) {
                mean = Double.valueOf(row.get(1).toString());
            }
            if (row.get(0).equals("stddev")) {
                stddev = Double.valueOf(row.get(1).toString());
            }
            if (row.get(0).equals("min")) {
                min = Double.valueOf(row.get(1).toString());
            }
            if (row.get(0).equals("max")) {
                max = Double.valueOf(row.get(1).toString());
            }
        }

        VBox boxList = new VBox();
        scene = new Scene(boxList, 400, 200);
        subStage.setScene(scene);
        subStage.setTitle("What Street?");
        boxList.getChildren().addAll(list, label);
        VBox.setVgrow(list, Priority.ALWAYS);

        label.setLayoutX(10);
        label.setLayoutY(115);
        label.setFont(Font.font("Verdana", 20));

        list.setItems(data);

        list.setCellFactory(new Callback<ListView<String>, ListCell<String>>() {
            @Override
            public ListCell<String> call(ListView<String> list) {
                return new UITextCell();
            }
        });

        final Integer stddeviation = stddev.intValue();
        final Integer average = mean.intValue();

        list.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<String>() {
            public void changed(ObservableValue<? extends String> ov, String old_val, String selectionText) {
                label.setText(selectionText);

                String v1 = selectionText.split(":")[0];
                String v2 = selectionText.split(":")[1];
                String v3 = selectionText.split(":")[2];

                System.out.println(v1 + " " + v2 + " " + v3);

                Point point= new Point(
                        BigDecimal.valueOf(Double.valueOf(selectionText.split(":")[1]))
                                .setScale(6, RoundingMode.HALF_UP).doubleValue(),
                        BigDecimal.valueOf(Double.valueOf(selectionText.split(":")[2]))
                                .setScale(6, RoundingMode.HALF_UP).doubleValue());
                prediction.setPoint(point);
                // prediction.sety(xy);

                Integer crimeCount = streetHashMap.get(selectionText.toLowerCase());

                if (crimeCount > stddeviation) {
                    series.getData().add(new XYChart.Data("Street", 10));
                    prediction.setStreet(10);
            
                    return;
                }

                if (crimeCount > average) {
                    series.getData().add(new XYChart.Data("Street", 8));
                    prediction.setStreet(8);
                    return;
                }

                if (crimeCount > average / 2) {
                    series.getData().add(new XYChart.Data("Street", 6));
                    prediction.setStreet(6);
                    return;
                }

                series.getData().add(new XYChart.Data("Street", 4));
                prediction.setStreet(4);

            }
        });
        subStage.show();
        series.getData().add(new XYChart.Data("Street", 0));
        prediction.setStreet(0);
 

        System.out.println("Displaying Street Stage");
    }
    
}
