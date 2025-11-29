
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import javax.xml.validation.Schema;

import org.apache.spark.SparkConf;
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
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.chart.PieChart;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.MapValueFactory;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import scala.collection.JavaConverters;
import utils.Utilities;

import java.nio.file.Files;

import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.layout.HBox;

import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import scala.collection.Seq;

/*



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
			
            

 */
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaFXClass extends Application {

    public static Dataset<Row> incidentData = null;
    public static SparkSession spark = null;

    static {

        final SparkConf sparkConf = new SparkConf()
                .setAppName("CrimeAvoidanceAI")
                .setMaster("local[*]") // Run locally with all available cores
                .set("spark.executor.memory", "2g") // Set executor memory
                .set("spark.driver.memory", "2g"); // Set driver memory

        spark = SparkSession.builder().appName("").config(sparkConf).getOrCreate();

    }

    @Override
    public void start(Stage subStage) {

        PieChart pieChart = new PieChart();

        PieChart.Data slice1 = new PieChart.Data("Desktop", 213);
        PieChart.Data slice2 = new PieChart.Data("Phone", 67);
        PieChart.Data slice3 = new PieChart.Data("Tablet", 36);

        pieChart.getData().add(slice1);
        pieChart.getData().add(slice2);
        pieChart.getData().add(slice3);

        ListView listView = new ListView();

        // listView.getItems().add("File2.csv");
        // listView.getItems().add("File3.csv");
        Button button1 = new Button("Select File");
        Button button2 = new Button("Button Number 2");

        FileChooser fileChooser = new FileChooser();
        fileChooser.setInitialDirectory(new File("C:\\Users\\jonrt\\OneDrive\\Desktop\\Projects\\MLDataNavigationUIApp\\data"));
        //fileChooser.setInitialFileName("myfile.txt");

        /*  fileChooser.getExtensionFilters().addAll(
            new FileChooser.ExtensionFilter("Text Files", "*.txt"),
            new FileChooser.ExtensionFilter("Data Files", "*.data")
        );*/
        TableView tableView = new TableView();

        button1.setOnAction(e -> {

            File selectedFile = fileChooser.showOpenDialog(subStage);
            System.out.println("Selected file: " + selectedFile.getAbsolutePath());

            listView.getItems().add(selectedFile.getName());

            incidentData = spark
                    .read()
                    .csv(selectedFile.getAbsolutePath());

            incidentData.show(5, false);
            // incidentData.printSchema();

            // Get the schema of the Dataset
            StructType schema = incidentData.schema();

            // Get the column names (header) from the schema
            String[] columnNames = schema.fieldNames();

            // Print the column names
            System.out.println("Column Names (Header):");
            for (String colName : columnNames) {

                System.out.println(colName.toString());

                TableColumn<Map, String> column1 = new TableColumn<>(colName.toString());

                column1.setCellValueFactory(new MapValueFactory<>(colName.toString().replaceAll(" ", "")));

                tableView.getColumns().add(column1);

            }

            Row row = incidentData.first();

            Seq<Object> rowSeq = row.toSeq();

            // Convert the Scala Seq to a Java List for easier iteration in Java
            java.util.List<Object> rowList = JavaConverters.seqAsJavaList(rowSeq);

            Map<String, Object> item1 = new HashMap<>();
            item1.put("_c0", "Randall");
            item1.put("_c1", "Kovic");
            item1.put("_c2", "Randall");
            item1.put("_c3", "Kovic");
            tableView.getItems().add(item1);

            /*  
			rowList.forEach( v -> {
				System.out.println( v  );



 
  
				// tableView.getItems().add(  v );

				


			} ); */
 /* 
			incidentData.collectAsList().forEach( r -> {
				Person person = new Person( r.getString(0), r.getString(1) );
				tableView.getItems().add( person );
			} );*/
        });


        /*
			TableColumn<Person, String> column1 = new TableColumn<>("First Name");
			
			column1.setCellValueFactory(new PropertyValueFactory<>("firstName"));


			TableColumn<Person, String> column2 = new TableColumn<>("Last Name");
			
			column2.setCellValueFactory(new PropertyValueFactory<>("lastName"));


			tableView.getColumns().add(column1);
			tableView.getColumns().add(column2);

			tableView.getItems().add(new Person("John", "Doe"));
			tableView.getItems().add(new Person("Jane", "Deer"));

 

         */
        //Label nameLabel = new Label("Enter your name:");
        TextField nameField = new TextField();
        Label feedbackLabel = new Label();

        button2.setOnAction(e -> {
            String name = nameField.getText();
            if (!name.isEmpty()) {
                feedbackLabel.setText("Hello, " + name + "!");
            } else {
                feedbackLabel.setText("Please enter a name.");
            }
        });

        VBox vbox1 = new VBox(button1, button2);
        vbox1.setPadding(new Insets(10, 20, 10, 10));
        //vbox1.setMargin(label, new Insets(10, 10, 10, 10));

        // vbox1.setWidth(200);
        // vbox1.setHeight(300);
        HBox hbox2 = new HBox(vbox1, listView, tableView);

        VBox vbox = new VBox(hbox2, nameField, feedbackLabel);

        Scene scene = new Scene(vbox, 800, 200);


        /*  
        HBox hbox = new HBox();
        hbox.setSpacing(10);
        hbox.setPadding(new Insets(10,20, 10,10));
        hbox.setMargin(label, new Insets(10, 10, 10, 10));
        
        ObservableList hboxList = hbox.getChildren();
        hboxList.addAll(label);
         */
        // Scene scene = new Scene(hbox, 100, 100);
        subStage.setTitle("MI");
        subStage.setScene(scene);

        subStage.setWidth(800);
        subStage.setHeight(300);

        subStage.show();

        /* 
        //Stage subStage = new Stage();
		//FlowPane root = new FlowPane();
		//Scene scene = new Scene(root, 300, 200);




        // Create UI controls
        Label nameLabel = new Label("Enter your name:");
        TextField nameField = new TextField();
        Button submitButton = new Button("Submit");
        Label feedbackLabel = new Label();

        // Handle button click event
        submitButton.setOnAction(e -> {
            String name = nameField.getText();
            if (!name.isEmpty()) {
                feedbackLabel.setText("Hello, " + name + "!");
            } else {
                feedbackLabel.setText("Please enter a name.");
            }
        });

        // Create a layout container (VBox for vertical arrangement)
        //VBox 
        // FlowPane root = new VBox(10); // Spacing of 10 pixels between elements
        VBox root = new VBox(10);

        ListView<String> list = new ListView<String>();
		ObservableList<String> data = FXCollections.observableArrayList();

		Label label = new Label();


        
        root.setPadding(new Insets(20)); // Padding around the container

        // Add controls to the layout container
        root.getChildren().addAll(nameLabel, nameField, submitButton, feedbackLabel);

        // Create a Scene and set it on the Stage
        Scene scene = new Scene(root, 300, 200); // Width, Height
        subStage.setTitle("Simple JavaFX UI");
        subStage.setScene(scene);
        subStage.show();
         */
    }

    public static void main(String[] args) {


        /* 


       Schema schema = new Schema.Parser().parse(
            ParquetWriteExample.class.getResourceAsStream("/user.avsc"));

        List<GenericData.Record> records = Arrays.asList(
            new GenericData.Record(schema) {{ put("name", "Alice"); put("favorite_number", 7); put("favorite_color", "blue"); }},
            new GenericData.Record(schema) {{ put("name", "Bob"); put("favorite_number", 13); put("favorite_color", null); }},
            new GenericData.Record(schema) {{ put("name", "Charlie"); put("favorite_number", null); put("favorite_color", "green"); }}
        );

        Path fileToWrite = new Path("users.parquet");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(fileToWrite)
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (GenericData.Record record : records) {
                writer.write(record);
            }
            System.out.println("Parquet file written successfully to " + fileToWrite);
        }

         */
        try {
            Class.forName("org.apache.spark.sql.SparkSession");

            /* 
		
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






             */
        } catch (Exception e) {
            System.out.println("Spark not found in classpath");

        }

        launch(args);
    }
}
