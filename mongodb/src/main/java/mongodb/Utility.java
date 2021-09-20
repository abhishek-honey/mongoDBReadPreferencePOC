package mongodb;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;

public class Utility {

	public static void main(String[] args) {

		String connStringPrimary = "mongodb+srv://Think:Jabra@cluster1.jt5g5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority";
		MongoCollection<Document> collectionPrimary = getCollection(connStringPrimary);

		String connStringSecondary = "mongodb+srv://Think:Jabra@cluster1.jt5g5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&readPreference=secondary";
		MongoCollection<Document> collectionSecondary = getCollection(connStringSecondary);

		String connStringSecondaryPreferred = "mongodb+srv://Think:Jabra@cluster1.jt5g5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&readPreference=secondaryPreferred";
		MongoCollection<Document> collectionSecondaryPreferred = getCollection(connStringSecondaryPreferred);


		System.out.println("---------Read Preference: default(primary)-----------");

		long sum = 0;
		int count = 100;
		
		for (int i = 0; i < count; i++) {
			sum += sendQuery(collectionPrimary, i);
		}
		System.out.println("Average: " + sum / count);
		

		System.out.println("--------------------Read Preference: secondary-------------------");
		
		sum = 0;
		
		for (int i = 0; i < count; i++) {
			sum += sendQuery(collectionSecondary, i);
		}
		System.out.println("Average: " + sum / count);


		System.out.println("--------------------Read Preference: secondaryPreferred-------------------");
		
		sum = 0;

		for (int i = 0; i < count; i++) {
			sum += sendQuery(collectionSecondaryPreferred, i);

		}
		System.out.println("Average: " + sum / count);

	}

	private static MongoCollection<Document> getCollection(String connStringSecondary) {
		ConnectionString connectionString = new ConnectionString(connStringSecondary);
		MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
		MongoClient mongoClient = MongoClients.create(settings);

		MongoDatabase database = mongoClient.getDatabase("POCDB");
		MongoCollection<Document> collection = database.getCollection("POCCOLL");
		return collection;
	}

	private static long sendQuery(MongoCollection<Document> collection, Integer value) {
		long start = System.currentTimeMillis();
		Document myDoc = collection.find(eq("fld0", value)).first();
		long end = System.currentTimeMillis();
		return end - start;
	}

}
