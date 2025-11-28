package MultiStage;

import org.apache.spark.sql.SQLContext;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.spark.SparkConf;
 
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import UI.UITextCell;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;
import javafx.scene.layout.FlowPane;
import javafx.scene.Scene;
import javafx.scene.control.ListView;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.Label;

/* 
import javafx.application.Application;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;

import javafx.event.Event;
import javafx.event.EventHandler;
import javafx.geometry.Pos;
import javafx.scene.Scene;
 
import javafx.geometry.Insets; 

import javafx.scene.control.Button; 
import javafx.scene.control.TextField; 
import javafx.scene.layout.HBox;
 

 
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.TextArea;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.scene.text.Font;

import javafx.util.Callback;
*/


import scala.Tuple2;
import utils.Utilities;

import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;



public class JavaClass {
    
    public JavaClass() {
        // Constructor
    }


/*
	public NextState(XYChart.Series series,			
			final JavaRDD<Row> incidentDataRowRDD,
			final SparkSession spark,
			final StructType schemaRF,
			final PipelineModel modelRF,
			final StructType schemaNB,
			final PipelineModel modelNB,
			final SQLContext sqlContext) {
		
		Stage subStage = new Stage();
		FlowPane root = new FlowPane();
		Scene scene = new Scene(root, 300, 200);

		ListView<String> list = new ListView<String>();
		ObservableList<String> data = FXCollections.observableArrayList();

		Label label = new Label();

		switch (state) {
		case 1:
			subStage.setTitle("Street");

			Map<String, Integer> streetHashMap = new HashMap<String, Integer>();

			JavaRDD<String> streetsRDD = incidentDataRowRDD.map(row -> row.getString(8).replaceAll("Block of ", "")
					.replaceAll("[0-9]+\\s", "").replaceAll("^\\s+", "") + " "
					+ new XY(row.getDouble(9), row.getDouble(10)));

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

					XY xy = new XY(
							BigDecimal.valueOf(Double.valueOf(selectionText.split(":")[1]))
									.setScale(6, RoundingMode.HALF_UP).doubleValue(),
							BigDecimal.valueOf(Double.valueOf(selectionText.split(":")[2]))
									.setScale(6, RoundingMode.HALF_UP).doubleValue());

					prediction.setXy(xy);

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
			state = 2;
			break;
			
		case 2:
			subStage.setTitle("Choose Age");

			List<String> ages = Arrays.asList("12-19", "20-24", "25-34", "35-49", "50-64", "64+");

			for (String age : ages) {
				data.add(age);
			}
			
			VBox boxAge = new VBox();
			scene = new Scene(boxAge, 200, 200);
			subStage.setScene(scene);
			subStage.setTitle("Choose Age");
			boxAge.getChildren().addAll(list, label);
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

			list.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<String>() {
				public void changed(ObservableValue<? extends String> ov, String old_val, String selectionText) {
				 
					if (selectionText.toLowerCase().equals("12-19")) {
						series.getData().add(new XYChart.Data("Age", 9));
						prediction.setAge(9);
					} else if (selectionText.toLowerCase().equals("20-24")) {
						series.getData().add(new XYChart.Data("Age", 7));
						prediction.setAge(7);
					} else if (selectionText.toLowerCase().equals("25-34")) {
						series.getData().add(new XYChart.Data("Age", 5));
						prediction.setAge(5);
					} else if (selectionText.toLowerCase().equals("35-49")) {
						series.getData().add(new XYChart.Data("Age", 3));
						prediction.setAge(3);
					} else if (selectionText.toLowerCase().equals("50-64")) {
						series.getData().add(new XYChart.Data("Age", 2));
						prediction.setAge(2);
					} else {
						series.getData().add(new XYChart.Data("Age", 1));
						prediction.setAge(1);
					}
				}
			});
			subStage.show();
			series.getData().add(new XYChart.Data("Age", 0));
			prediction.setAge(0 );

			state = 3;
			break;
		case 3:
			subStage.setTitle("Choose Ethnicity");

			List<String> races = Arrays.asList("Black", "White", "Hispanic", "Asian", "Native American",
					"Other ethnicity", "Mixed ethnicity");

			for (String race : races) {
				data.add(race);
			}

			System.out.println("race: " + races.size());

			VBox boxRace = new VBox();
			scene = new Scene(boxRace, 200, 200);
			subStage.setScene(scene);
			subStage.setTitle("Choose Ethnicity");
			boxRace.getChildren().addAll(list, label);
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

			list.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<String>() {
				public void changed(ObservableValue<? extends String> ov, String old_val, String selectionText) {
					
					if (selectionText.toLowerCase().equals("black")) {
						series.getData().add(new XYChart.Data("Ethnicity", 7));
						prediction.setEthnicity(7);
					} else if (selectionText.toLowerCase().equals("white")) {
						series.getData().add(new XYChart.Data("Ethnicity", 4));
						prediction.setEthnicity(4);
					} else if (selectionText.toLowerCase().equals("hispanic")) {
						series.getData().add(new XYChart.Data("Ethnicity", 3));
						prediction.setEthnicity(3);
					} else if (selectionText.toLowerCase().equals("asian")) {
						series.getData().add(new XYChart.Data("Ethnicity", 3));
						prediction.setEthnicity(3);
					} else if (selectionText.toLowerCase().equals("Native American")) {
						series.getData().add(new XYChart.Data("Ethnicity", 2));
						prediction.setEthnicity(2);
					} else if (selectionText.toLowerCase().equals("Other ethnicity")) {
						series.getData().add(new XYChart.Data("Ethnicity", 1));
						prediction.setEthnicity(1);
					} else if (selectionText.toLowerCase().equals("Mixed ethnicity")) {
						series.getData().add(new XYChart.Data("Ethnicity", 1));
						prediction.setEthnicity(1);
					} else {
						series.getData().add(new XYChart.Data("Ethnicity", 1));
					}
				}
			});
			subStage.show();
			
			prediction.setEthnicity(0 );
			series.getData().add(new XYChart.Data("Ethnicity",0));
			
			state = 4;
			break;
		case 4:
			subStage.setTitle("Choose Gender");

			List<String> genders = Arrays.asList("Male", "Female");

			for (String gender : genders) {
				data.add(gender);
			}

			VBox boxGender = new VBox();
			scene = new Scene(boxGender, 200, 200);
			subStage.setScene(scene);
			subStage.setTitle("Choose Gender");
			boxGender.getChildren().addAll(list, label);
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

			list.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<String>() {
				public void changed(ObservableValue<? extends String> ov, String old_val, String selectionText) {
					label.setText(selectionText);
					
					if (selectionText.toLowerCase().equals("male")) {
						series.getData().add(new XYChart.Data("Gender", 3));
						prediction.setGender(3);
					} else {
						series.getData().add(new XYChart.Data("Gender", 1));
						prediction.setGender(1);
					}
				}
			});
			subStage.show();

			series.getData().add(new XYChart.Data("Gender", 0));
			prediction.setGender(0);

			state = 5;
			break;
		case 5:
			subStage.setTitle("Time");

			List<String> times = Arrays.asList("12 AM", "1 AM", "2 AM", "3 AM", "4 AM", "5 AM", "6 AM", "7 AM", "8 AM",
					"9 AM", "10 AM", "11 AM", "12 PM", "1 PM", "2 PM", "3 PM", "4 PM", "5 PM", "6 PM", "7 PM", "8 PM",
					"9 PM", "10 PM", "11 PM");

			for (String time : times) {
				data.add(time);
			}

			VBox boxTime = new VBox();
			scene = new Scene(boxTime, 200, 200);
			subStage.setScene(scene);
			subStage.setTitle("What Time?");
			boxTime.getChildren().addAll(list, label);
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

			list.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<String>() {
				public void changed(ObservableValue<? extends String> ov, String old_val, String selectionText) {
					label.setText(selectionText);

					if (selectionText.toUpperCase().equals("12 AM")) {
						series.getData().add(new XYChart.Data("Hour", 9));
						prediction.setTime(7);
						prediction.setOfficialTime(0.0);

					} else if (selectionText.toUpperCase().equals("1 AM")) {
						series.getData().add(new XYChart.Data("Hour", 9));
						prediction.setTime(7);
						prediction.setOfficialTime(1.0);
					} else if (selectionText.toUpperCase().equals("2 AM")) {
						series.getData().add(new XYChart.Data("Hour", 9));
						prediction.setTime(7);
						prediction.setOfficialTime(2.0);
					} else if (selectionText.toUpperCase().equals("3 AM")) {
						series.getData().add(new XYChart.Data("Hour", 5));
						prediction.setTime(5);
						prediction.setOfficialTime(3.0);
					} else if (selectionText.toUpperCase().equals("4 AM")) {
						series.getData().add(new XYChart.Data("Hour", 2));
						prediction.setTime(2);
						prediction.setOfficialTime(4.0);
					} else if (selectionText.toUpperCase().equals("5 AM")) {
						series.getData().add(new XYChart.Data("Hour", 1));
						prediction.setTime(2);
						prediction.setOfficialTime(5.0);
					} else if (selectionText.toUpperCase().equals("6 AM")) {
						series.getData().add(new XYChart.Data("Hour", 1));
						prediction.setTime(3);
						prediction.setOfficialTime(6.0);
					} else if (selectionText.toUpperCase().equals("7 AM")) {
						series.getData().add(new XYChart.Data("Hour", 1));
						prediction.setTime(3);
						prediction.setOfficialTime(7.0);
					} else if (selectionText.toUpperCase().equals("8 AM")) {
						series.getData().add(new XYChart.Data("Hour", 3));
						prediction.setTime(3);
						prediction.setOfficialTime(8.0);
					} else if (selectionText.toUpperCase().equals("9 AM")) {
						series.getData().add(new XYChart.Data("Hour", 3));
						prediction.setTime(4);
						prediction.setOfficialTime(9.0);
					} else if (selectionText.toUpperCase().equals("10 AM")) {
						series.getData().add(new XYChart.Data("Hour", 3));
						prediction.setTime(4);
						prediction.setOfficialTime(10.0);
					} else if (selectionText.toUpperCase().equals("11 AM")) {
						series.getData().add(new XYChart.Data("Hour", 3));
						prediction.setTime(4);
						prediction.setOfficialTime(11.0);
					} else if (selectionText.toUpperCase().equals("12 PM")) {
						series.getData().add(new XYChart.Data("Hour", 4));
						prediction.setTime(4);
						prediction.setOfficialTime(12.0);
					} else if (selectionText.toUpperCase().equals("1 PM")) {
						series.getData().add(new XYChart.Data("Hour", 4));
						prediction.setTime(4);
						prediction.setOfficialTime(13.0);
					} else if (selectionText.toUpperCase().equals("2 PM")) {
						series.getData().add(new XYChart.Data("Hour", 3));
						prediction.setTime(4);
						prediction.setOfficialTime(14.0);
					} else if (selectionText.toUpperCase().equals("3 PM")) {
						series.getData().add(new XYChart.Data("Hour", 4));
						prediction.setTime(4);
						prediction.setOfficialTime(15.0);
					} else if (selectionText.toUpperCase().equals("4 PM")) {
						series.getData().add(new XYChart.Data("Hour", 3));
						prediction.setTime(4);
						prediction.setOfficialTime(16.0);
					} else if (selectionText.toUpperCase().equals("5 PM")) {
						series.getData().add(new XYChart.Data("Hour", 3));
						prediction.setTime(5);
						prediction.setOfficialTime(17.0);
					} else if (selectionText.toUpperCase().equals("6 PM")) {
						series.getData().add(new XYChart.Data("Hour", 4));
						prediction.setTime(5);
						prediction.setOfficialTime(18.0);
					} else if (selectionText.toUpperCase().equals("7 PM")) {
						series.getData().add(new XYChart.Data("Hour", 5));
						prediction.setTime(5);
						prediction.setOfficialTime(19.0);
					} else if (selectionText.toUpperCase().equals("8 PM")) {
						series.getData().add(new XYChart.Data("Hour", 6));
						prediction.setTime(5);
						prediction.setOfficialTime(20.0);
					} else if (selectionText.toUpperCase().equals("9 PM")) {
						series.getData().add(new XYChart.Data("Hour", 7));
						prediction.setTime(5);
						prediction.setOfficialTime(21.0);
					} else if (selectionText.toUpperCase().equals("10 PM")) {
						series.getData().add(new XYChart.Data("Hour", 8));
						prediction.setTime(6);
						prediction.setOfficialTime(22.0);
					} else if (selectionText.toUpperCase().equals("11 PM")) {
						series.getData().add(new XYChart.Data("Hour", 8));
						prediction.setTime(6);
						prediction.setOfficialTime(23.0);
					} else {
						series.getData().add(new XYChart.Data("Hour", 0));
						prediction.setTime(0);
						prediction.setOfficialTime(0.0);
					}
				}
			});
			
			series.getData().add(new XYChart.Data("Hour", 0));
			prediction.setTime(0);
			subStage.show();
			state = 6;
			break;
		case 6:

			subStage.setTitle("What are your plans?");

			final TextArea textArea = new TextArea();
			
			Button button = new Button("Compute");
			
			BorderPane borderPane = new BorderPane();
			borderPane.setTop(textArea);
			
			button.setOnAction(new EventHandler() {
				@Override
				public void handle(Event event) {

					String testString = textArea.getText();
					List<Row> inputTestTextRow = new ArrayList<Row>();
					inputTestTextRow
							.add(RowFactory.create("30000", "UNKNOWN", utils.Utilities.cleanText(testString), 1));
					
					Dataset<Row> testingData = sqlContext.createDataFrame(inputTestTextRow, schemaNB);
					Dataset<Row> testPredictions = modelNB.transform(testingData);
					Double textPediction = Double.valueOf(testPredictions.collectAsList().get(0).get(9).toString());
					DenseVector vector = (DenseVector) testPredictions.collectAsList().get(0).get(7);

					Double fraction = Math.abs(Math.abs(vector.toArray()[0]) - Math.abs(vector.toArray()[1]))
							/ (Math.abs(vector.toArray()[0]) + Math.abs(vector.toArray()[1]));

					Integer rating = 0;

					System.out.println("Fraction: " + fraction);

					if (textPediction == 1.0 && fraction < 0.02) {
						prediction.setPlan(3);
						series.getData().add(new XYChart.Data("Plans", 3));
					} else if (textPediction == 1.0 && fraction >= 0.02 && fraction < 0.06) {
						prediction.setPlan(6);
						series.getData().add(new XYChart.Data("Plans", 6));
					} else if (textPediction == 1.0 && fraction >= 0.06 && fraction < 0.09) {
						prediction.setPlan(9);
						series.getData().add(new XYChart.Data("Plans", 9));
					} else {
						prediction.setPlan(11);
						series.getData().add(new XYChart.Data("Plans", 12));
					}

					if (textPediction == 0.0) {
						prediction.setPlan(0);
						series.getData().add(new XYChart.Data("Plans", 0));
					}

				}
			});

			borderPane.setBottom(button );
			
			scene = new Scene(borderPane, 300, 200);
			subStage.setScene(scene);
			subStage.show();
			
			series.getData().add(new XYChart.Data("Plans", 0));

			state = 7;
			break;
		case 7:
			subStage.setTitle("Find your score");

			final Label threatLevelLabel = new Label();
			final Label threatLevelTextLabel = new Label();
			final Label crimeLabel = new Label();
			Button buttonScore = new Button("Threat Level");
			
			buttonScore.setOnAction(new EventHandler() {
				@Override
				public void handle(Event event) {
					Integer totalMaxThreat = 50;

					Double doubleValue = 0.0;
					Double predictionValue = Double.valueOf(prediction.getTotal());
					String output = "";
					
					if(predictionValue/totalMaxThreat > 0.85) {
						output = "Threat level: Warning! Extreme threat. Avoid situation at all cost.";
					} else if (predictionValue/totalMaxThreat <= 0.85 && predictionValue/totalMaxThreat > 0.60) {
						output = "Threat level: High! Be extremely watchful.";
					} else if (predictionValue/totalMaxThreat <= 0.60 && predictionValue/totalMaxThreat > 0.38) {
						output = "Threat level: Advisory!";
					} else {
						output = "Threat level: Low!";
					}

					threatLevelLabel.setText(prediction.getTotal() + " of " + totalMaxThreat);
					threatLevelTextLabel.setText(output);
				}
			});
			
			Button buttonCrimePrediction = new Button("Crime Prediction");
			
			buttonCrimePrediction.setOnAction(new EventHandler() {
				@Override
				public void handle(Event event) {

					List<Row> inputTestTextRow = new ArrayList<Row>();
					System.out.println(prediction.officialTime + " " + prediction.xy.X + " " + prediction.xy.Y);
					
					inputTestTextRow.add( RowFactory.create(			
							"950060275", 
							"ASSAULT", 
							"LOST PROPERTY", 
							Double.valueOf("1.0"),
							Double.valueOf("1.0"),
							Double.valueOf(prediction.officialTime),
							"MISSION", 
							"NONE", 	
							"MARKET ST",
							prediction.xy.X,
							prediction.xy.Y,
							"(37.7617007179518, -122.42158168137)",
							"15006027571000" ) );

					Dataset<Row> testData = sqlContext.createDataFrame(inputTestTextRow, schemaRF);
					
					// Make predictions
					Dataset<Row> predictions = modelRF.transform(testData);

					Dataset<Row> predictionsData = predictions.select( 
						org.apache.spark.sql.functions.col("Category"), 
						org.apache.spark.sql.functions.col("labelIndexed") ,
						org.apache.spark.sql.functions.col("Address") ,
						org.apache.spark.sql.functions.col("prediction") 
					);

					List<Row> predictionRows = predictionsData.javaRDD().map(row->{ 
						return RowFactory.create( 					
							row.get(0), 
							row.get(1), 
							row.get(2),
							categoryInversed.get(Double.valueOf(row.get(3).toString()))
						);
					}).collect();
					 
					String crime = "";
					
					for (Row rowv : predictionRows) {
						crime = rowv.getString(3);
					}
					
					crimeLabel.setText("Watch out for " + crime);
					System.out.println(crime);
				}
			});

			VBox buttonVbox = new VBox();
			buttonVbox.setSpacing(5);
			buttonVbox.setMargin(buttonScore, new Insets(5, 5, 5, 5));
			buttonVbox.setMargin(buttonCrimePrediction, new Insets(5, 5, 5, 5));
			ObservableList buttonVboxList = buttonVbox.getChildren();
			buttonVboxList.addAll(buttonScore, buttonCrimePrediction);

			VBox labelVbox = new VBox();
			labelVbox.setSpacing(5);
			labelVbox.setMargin(threatLevelLabel, new Insets(5, 5, 5, 5));
			labelVbox.setMargin(threatLevelTextLabel, new Insets(5, 5, 5, 5));
			labelVbox.setMargin(crimeLabel, new Insets(5, 5, 5, 5));
			ObservableList labelVboxList = buttonVbox.getChildren();
			labelVboxList.addAll(threatLevelLabel, threatLevelTextLabel,crimeLabel );


			BorderPane borderPanel = new BorderPane();
			
			borderPanel.setLeft(buttonVbox );
			borderPanel.setRight(labelVbox );
			
			
			scene = new Scene(borderPanel,400, 260);
			subStage.setScene(scene);
			subStage.show();

			state = 10;
			break;

		default:
			state = 10;
			break;
		}
	}
*/	

    public static void main(String[] args) throws Exception {

		//String javaVersion = System.getProperty("java.version");
        // String javafxVersion = System.getProperty("javafx.version");
        //Label l = new Label("Hello, JavaFX " + javafxVersion + ", running on Java " + javaVersion + ".");


		// Label label = new Label("Loading...");
       // HBox hbox = new HBox();
       /// hbox.setSpacing(10);
      //  hbox.setPadding(new Insets(10,20, 10,10));
        //hbox.setMargin(label, new Insets(10, 10, 10, 10));
        
        //ObservableList hboxList = hbox.getChildren();
        //hboxList.addAll(label);
		
		//Scene scene = new Scene(hbox, 100, 100);

		//stage.setTitle("Crime Avoidance AI");
		//stage.setScene(scene);
		// stage.show();
		
		Dataset<Row> incidentData = null;
		StructType schemaNB;
		//PipelineModel modelNB;
		//Map<String, Double> category;
		//Map<Double, String> categoryInversed;
		// JavaSparkContext javaSparkContext;
		//JavaRDD<Row> incidentDataRowRDD;
	 
		Dataset<Row> incidentFilteredTrainData;
		//PipelineModel modelRF;

      
		final SparkConf sparkConf = new SparkConf()
					.setAppName("CrimeAvoidanceAI")
					.setMaster("local[*]") // Run locally with all available cores
					.set("spark.executor.memory", "2g") // Set executor memory
					.set("spark.driver.memory", "2g"); // Set driver memory

   
        final SparkSession spark = SparkSession.builder().appName("").config(sparkConf).getOrCreate();
  
		incidentData = spark
				.read()
				.csv(utils.Utilities.getPath() + "Police_Department_Incidents.data");


	 	incidentData.show(5, false);
		incidentData.printSchema();

 

		RDD<Row> rows = incidentData.javaRDD().filter(row -> row.length()== 13).map((row) -> {
			return RowFactory.create(
					row.get(0), 
					row.get(1), 
					row.get(2), 
					utils.Utilities.DayOfWeek(row.get(3)),
					utils.Utilities.RoundDate(row.get(4)), 
					utils.Utilities.RoundTime(row.get(5)), 
					row.get(6), 
					row.get(7), 
					utils.Utilities.CleanStreetName(row.get(8).toString()),
					BigDecimal.valueOf(Double.valueOf(row.get(9).toString())).setScale(6, RoundingMode.HALF_UP).doubleValue(),
					BigDecimal.valueOf(Double.valueOf(row.get(10).toString())).setScale(6, RoundingMode.HALF_UP).doubleValue(),
					row.get(11), 
					row.get(12)
			); 
		}).rdd();    


		List<String> tableColumnsRF = Arrays.asList(
				"IncidntNum","Category","Descript","DayOfWeek","Date","Time","PdDistrict",
				"Resolution","Address","X","Y","Location","PdId"  );
		
		StructType schemaRF = Utilities.createSchema(tableColumnsRF);		

 		incidentFilteredTrainData = spark.createDataFrame(rows , schemaRF);
 

	
 

		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{ "Date", "Time", "X", "Y"}).setOutputCol("features");
		StringIndexer indexer = new StringIndexer().setInputCol("Category").setOutputCol("labelIndexed");
 
		
		
		RandomForestClassifier randomForestClassifier = new RandomForestClassifier().setLabelCol("labelIndexed").setFeaturesCol("features");
		
	
		Pipeline pipelineRF = new Pipeline().setStages(new PipelineStage[] {assembler, indexer, randomForestClassifier});
		
		System.out.println("Preparing modelRF...");
		
		PipelineModel modelRF = pipelineRF.fit(incidentFilteredTrainData);

		List<String> tableColumns = Arrays.asList("id","topic","text","label");
		schemaNB = utils.Utilities.createSchemaNB(tableColumns);		

		List<Row> inputTextRow = new ArrayList<Row>();
	

 
		//try {
			// NON CRIME
 
		String content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "good/d1.txt"))));
		inputTextRow.add( RowFactory.create("00001", "TRAVEL", content ,0) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "good/d2.txt"))));
		
		inputTextRow.add( RowFactory.create("00002", "TRAVEL", content ,0) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "good/d3.txt"))));
		
		inputTextRow.add( RowFactory.create("00003", "TRAVEL", content ,0) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "good/d4.txt"))));
		inputTextRow.add( RowFactory.create("00004", "TRAVEL", content ,0) );
		

		// CRIME
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "bad/d1.txt"))));
		inputTextRow.add( RowFactory.create("10003", "CRIME", content ,1) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "bad/d2.txt"))));
		inputTextRow.add( RowFactory.create("10003", "CRIME", content ,1) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "bad/d3.txt"))));
		inputTextRow.add( RowFactory.create("10003", "CRIME", content ,1) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "bad/d4.txt"))));
		inputTextRow.add( RowFactory.create("10003", "CRIME", content ,1) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "bad/d5.txt"))));
		inputTextRow.add( RowFactory.create("10003", "CRIME", content ,1) );
		//} catch (Exception e) {
			
		//}


		Dataset<Row> trainingData = spark.createDataFrame(inputTextRow, schemaNB);

		RegexTokenizer tokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("wordsoutput");
		
		StopWordsRemover remover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol()).setOutputCol("words");
		
		HashingTF hashingTF = new HashingTF()
				  .setInputCol(remover.getOutputCol()) 
				  .setOutputCol("features")
				  .setNumFeatures(5000);		  

		NaiveBayes naiveBayes = new NaiveBayes().setSmoothing(1.0).setModelType("multinomial"); 

		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{tokenizer, remover, hashingTF, naiveBayes});

		System.out.println("Preparing modelNB...");
		
		PipelineModel modelNB = pipeline.fit(trainingData);
		
		System.out.println("Done modelNB...");

		//
		 
		// new NextState(series, incidentDataRowRDD, spark, schemaRF, modelRF, schemaNB, modelNB, sqlContext);
		//new NextState(series, incidentDataRowRDD, spark, schemaRF, modelRF, schemaNB, modelNB, sqlContext);
		//new NextState(series, incidentDataRowRDD, spark, schemaRF, modelRF, schemaNB, modelNB, sqlContext);
		//new NextState(series, incidentDataRowRDD, spark, schemaRF, modelRF, schemaNB, modelNB, sqlContext);
		//new NextState(series, incidentDataRowRDD, spark, schemaRF, modelRF, schemaNB, modelNB, sqlContext);
		//new NextState(series, incidentDataRowRDD, spark, schemaRF, modelRF, schemaNB, modelNB, sqlContext);
		//new NextState(series, incidentDataRowRDD, spark, schemaRF, modelRF, schemaNB, modelNB, sqlContext);
		
		// spark.stop();
		System.out.println("Went this far");

    }

	public void runRDD( 
			final Dataset<Row> incidentData, 
			final SparkSession spark,
			final StructType schemaRF,
			final PipelineModel modelRF,
			final StructType schemaNB,
			final PipelineModel modelNB 
			) {


		    //subStage.setTitle("Street");

			Map<String, Integer> streetHashMap = new HashMap<String, Integer>();

			JavaRDD<String> streetsRDD = incidentData.javaRDD().map(row -> row.getString(8).replaceAll("Block of ", "")
					.replaceAll("[0-9]+\\s", "").replaceAll("^\\s+", "") + " "
					+ new XY(row.getDouble(9), row.getDouble(10)));

			JavaPairRDD<String, Integer> pairs = streetsRDD.mapToPair(s -> new Tuple2(s, 1));
			List<Tuple2<String, Integer>> counts = pairs.reduceByKey((a, b) -> a + b).sortByKey().collect();

			for (Tuple2<String, Integer> street : counts) {
				if (street._1.contains("/")) {
					//data.add(street._1);
				}

				// streetHashMap.put(street._1.toLowerCase(), street._2.intValue());
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

			System.out.println("Fix from here forward: ");
			//VBox boxList = new VBox();
			//scene = new Scene(boxList, 400, 200);
			//subStage.setScene(scene);
			//subStage.setTitle("What Street?");
			//boxList.getChildren().addAll(list, label);
			//VBox.setVgrow(list, Priority.ALWAYS);

			//label.setLayoutX(10);
			//label.setLayoutY(115);
			//label.setFont(Font.font("Verdana", 20));

			//list.setItems(data);

			/* 			list.setCellFactory(new Callback<ListView<String>, ListCell<String>>() {
				@Override
				public ListCell<String> call(ListView<String> list) {
					return new UITextCell();
				}
			});*/

		//	final Integer stddeviation = stddev.intValue();
	//		final Integer average = mean.intValue();
/* 
			list.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<String>() {
				public void changed(ObservableValue<? extends String> ov, String old_val, String selectionText) {
					label.setText(selectionText);

					String v1 = selectionText.split(":")[0];
					String v2 = selectionText.split(":")[1];
					String v3 = selectionText.split(":")[2];

					System.out.println(v1 + " " + v2 + " " + v3);

					XY xy = new XY(
							BigDecimal.valueOf(Double.valueOf(selectionText.split(":")[1]))
									.setScale(6, RoundingMode.HALF_UP).doubleValue(),
							BigDecimal.valueOf(Double.valueOf(selectionText.split(":")[2]))
									.setScale(6, RoundingMode.HALF_UP).doubleValue());

					prediction.setXy(xy);

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
			state = 2;
			//break;

*/
	}



 
	public void NextState(XYChart.Series series,			
			final JavaRDD<Row> incidentDataRowRDD,
			final SparkSession spark,
			final StructType schemaRF,
			final PipelineModel modelRF,
			final StructType schemaNB,
			final PipelineModel modelNB,
			final SQLContext sqlContext) 
	{
		
		Stage subStage = new Stage();
		FlowPane root = new FlowPane();
		Scene scene = new Scene(root, 300, 200);

		ListView<String> list = new ListView<String>();
		ObservableList<String> data = FXCollections.observableArrayList();

		Label label = new Label();
 /*
		switch (state) {
		case 1:
			subStage.setTitle("Street");

			Map<String, Integer> streetHashMap = new HashMap<String, Integer>();

			JavaRDD<String> streetsRDD = incidentDataRowRDD.map(row -> row.getString(8).replaceAll("Block of ", "")
					.replaceAll("[0-9]+\\s", "").replaceAll("^\\s+", "") + " "
					+ new XY(row.getDouble(9), row.getDouble(10)));

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

					XY xy = new XY(
							BigDecimal.valueOf(Double.valueOf(selectionText.split(":")[1]))
									.setScale(6, RoundingMode.HALF_UP).doubleValue(),
							BigDecimal.valueOf(Double.valueOf(selectionText.split(":")[2]))
									.setScale(6, RoundingMode.HALF_UP).doubleValue());

					prediction.setXy(xy);

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
			state = 2;
			break;
		 }*/

	}
}
