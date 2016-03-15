/**
 * Copyright 2016 Cleosson Souza
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.avro.sample;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.FileInputStream;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.Schema;

import com.avro.sample.entity.Book;

public class FileSerializationExample {

    public static void main(final String[] args) throws IOException {
        Book book1 = Book.newBuilder().setId(123).setName("Programming is fun").setCategory("Fiction").build();
        Book book2 = new Book("Some book", 456, "Horror");
        Book book3 = new Book();
        book3.setName("And another book");
        book3.setId(789);
        File store = File.createTempFile("book", ".avro");

        // serializing
        System.out.println("Serializing books to temp file: " + store.getPath());
        DatumWriter<Book> bookDatumWriter = new SpecificDatumWriter<Book>(Book.class);
        DataFileWriter<Book> bookFileWriter = new DataFileWriter<Book>(bookDatumWriter);
        bookFileWriter.create(book1.getSchema(), store);
        bookFileWriter.append(book1);
        bookFileWriter.append(book2);
        bookFileWriter.append(book3);
        bookFileWriter.close();

        // Deserializing using the generated code
        System.out.println("Deserializing using the generated code from temp file: " + store.getPath());
        DatumReader<Book> bookDatumReader = new SpecificDatumReader<Book>(Book.class);
        DataFileReader<Book> bookFileReader = new DataFileReader<Book>(store, bookDatumReader);
        while (bookFileReader.hasNext()) {
            Book b1 = bookFileReader.next();
            System.out.println("Deserialized from file: " + b1);
        }

        // Deserializing without using the generated code
        System.out.println("Deserializing without the generated code from temp file: " + store.getPath());
        FileInputStream fis = new FileInputStream(store);
        byte bytes[] = new byte[(int)store.length()];
        fis.read(bytes, 0, (int)store.length());
                        
        // Get the schema
        GenericDatumReader<GenericRecord> datumReaderSchema = new GenericDatumReader<GenericRecord>();
        SeekableByteArrayInput avroInputStream = new SeekableByteArrayInput(bytes);
        DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(avroInputStream, datumReaderSchema);
        Schema schema = fileReader.getSchema();
        System.out.println("Schema: " + schema.toString());
            
        GenericRecord record = null;
        List<GenericRecord> records = new ArrayList<GenericRecord> ();
        System.out.println("Records in the file:");
        while (fileReader.hasNext()) {
            record = fileReader.next();
            records.add(record);
            System.out.println(record);
        }

        System.out.println("Read " + records.size() + " records");
        System.out.println("Read name = " + records.get(0).get("name"));
    }
}
