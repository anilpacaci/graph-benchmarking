/* 
 * Copyright (C) 2015-2016 Stanford University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package ca.uwaterloo.cs.kafka.ca.uwaterloo.cs.kafka.updates;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

// modified from https://github.com/PlatformLab/ldbc-snb-impls by Jonathan Ellithorpe @cs.stanford.edu
public class UpdatesProducer
{

  static SimpleDateFormat creationDateDateFormat =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  static SimpleDateFormat joinDateDateFormat =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  static SimpleDateFormat birthdayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
  static String inputBaseDir = "/Users/alicezhou/Development/ldbc_snb_datagen/social_network";

  static String updatePersonFiles[] = {
      "updateStream_0_0_person.csv",
      "updateStream_0_1_person.csv",
      "updateStream_0_2_person.csv",
      "updateStream_0_3_person.csv",
      "updateStream_0_4_person.csv",
      "updateStream_0_5_person.csv",
      "updateStream_0_6_person.csv",
      "updateStream_0_7_person.csv",
      "updateStream_0_8_person.csv",
      "updateStream_0_9_person.csv",
      "updateStream_0_10_person.csv",
      "updateStream_0_11_person.csv",
      "updateStream_0_12_person.csv",
      "updateStream_0_13_person.csv",
      "updateStream_0_14_person.csv",
      "updateStream_0_15_person.csv"
  };

  static String updateForumFiles[] = {
      "updateStream_0_0_forum.csv",
      "updateStream_0_1_forum.csv",
      "updateStream_0_2_forum.csv",
      "updateStream_0_3_forum.csv",
      "updateStream_0_4_forum.csv",
      "updateStream_0_5_forum.csv",
      "updateStream_0_6_forum.csv",
      "updateStream_0_7_forum.csv",
      "updateStream_0_8_forum.csv",
      "updateStream_0_9_forum.csv",
      "updateStream_0_10_forum.csv",
      "updateStream_0_11_forum.csv",
      "updateStream_0_12_forum.csv",
      "updateStream_0_13_forum.csv",
      "updateStream_0_14_forum.csv",
      "updateStream_0_15_forum.csv"
  };

  public UpdatesProducer() {}

  public static void main(String[] args) throws IOException {
    Properties prop = new Properties();
    InputStream input = null;

    try {
      input = UpdatesProducer.class.getClassLoader().getResourceAsStream("producer-generic.properties");
      if (input == null) {
        System.out.println("Sorry, unable to find producer properties file");
      } else {
        //load a properties file from class path, inside static method
        prop.load(input);
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      if (input != null) input.close();
    }
    creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    joinDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    birthdayDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

    prop.setProperty("value.serializer", "ca.uwaterloo.cs.kafka.updates.UpdateSerializer");
    Producer<String, Update> updateProducer = new KafkaProducer<>(prop);

    try {
      produceUpdates(updateProducer, updatePersonFiles, "updates");
      System.out.println("done vertices");
    } catch (Exception e) {
      System.out.println("Exception: " + e);
      e.printStackTrace();
    }
  }

  private static void produceUpdates(Producer<String, Update> producer, String[] files, String topic) {
    try {
      for (String fileName : files) {
        System.out.println("Loading updates file " + fileName + " ");
        int propCounter = 0;
        String fileNameParts[] = fileName.split("_");
        String updateType = fileNameParts[2];
        List<String> lines = Files.readAllLines(Paths.get(inputBaseDir + "/" + fileName));

        for (int i = 1; i < lines.size(); ++i) {
          String line = lines.get(i);
          Update update = new Update();
          update.updateQuery = updateType;
          update.csvLine = line;

          ProducerRecord<String, Update> record = new ProducerRecord<>(topic, Integer.toString(propCounter), update);
          producer.send(record).get();
          propCounter++;
        }
      }
    } catch (IOException | InterruptedException | ExecutionException e) {
      e.printStackTrace();
    } finally {
      producer.close();
    }
  }

/*
Update Query Descriptions
   1. Add Person
     • Description: Add a Person to the social network.
     • Parameters:
     Person.id
     Person.firstName Person.lastName
     Person.gender
     Person.birthDay Person.creationDate Person.locationIp Person.browserUsed Person-isLocatedIn->City.id Person.speaks
     Person.emails Person-hasInterest->Tag.id
     { Person-studyAt->University.id, Person-studyAt->.classYear }
     { Person-workAt->Company.id, Person-workAt->.workFrom }

     ID
     String String String Date DateTime String String
     ID
     { String } { String } {ID}
     {ID, 32-bit Integer} {ID, 32-bit Integer}

   2. Add Post Like
     • Description: Add a Like to a Post of the social network.
     • Parameters:
     Person.id ID
     Post.id ID
     Person-likes->.creationDate DateTime

   3. Add Comment Like
     • Description: Add a Like to a Comment of the social network. • Parameters:
     Person.id ID Comment.id ID Person-likes->.creationDate DateTime

   4. Add Forum
     • Description: Add a Forum to the social network.
     • Parameters: Forum.id
     Forum.title
     Forum.creationDate Forum-hasModerator->Person.id Forum-hasTag->Tag.id
     ID // person 1 String // person 2 DateTime
     { ID }
     { ID }
     Page 32 of (39) LDBC Social Network Benchmark (SNB) - v0.2.3 First Public Draft Release

   5. Add Forum Membership
     • Description: Add a Forum membership to the social network.
     • Parameters:
     Person.id ID
     Person-hasMember->Forum.id ID Person-hasMember->.joinDate DateTime

   6. Add Post
     • Description: Add a Post to the social network.
     • Parameters: Post.id
     Post.imageFile Post.creationDate Post.locationIp Post.browserUsed Post.language
     Post.content
     Post.length Post-hasCreator->Person.id Forum-containerOf->Post.id Post-isLocatedIn->Country.id {Post-hasTag->Tag.id}
     ID
     String DateTime String
     String
     String
     Text
     32-bit Integer ID
     ID
     ID
     {ID}

   7. Add Comment
     • Description: Add a Comment replying
     to a Post/Comment to the social network.
     • Parameters:
     Comment.id ID
     Comment.creationDate DateTime Comment.locationIp String Comment.browserUsed String Comment.content Text Comment.length 32-bit Integer Comment-hasCreator->Person.id ID Comment-isLocatedIn->Country.id ID Comment-replyOf->Post.id ID Comment-replyOf->Comment.id ID {Comment-hasTag->Tag.id} {ID}
     // -1 if the comment is a reply of a comment. // -1 if the comment is a reply of a post.

   8. Add Friendship
     • Description: Add a friendship relation to the social network
     • Parameters:
     Person.id ID // person 1
     Person.id ID // person 2 Person-knows->.creationDate DateTime
*/
}
