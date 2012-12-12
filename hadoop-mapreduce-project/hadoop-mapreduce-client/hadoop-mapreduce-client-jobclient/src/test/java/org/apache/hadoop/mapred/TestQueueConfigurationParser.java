/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.ls.LSSerializer;
import static org.junit.Assert.*;

import org.junit.Test;

import com.sun.org.apache.xerces.internal.dom.DOMImplementationImpl;

public class TestQueueConfigurationParser {
/**
 * test xml generation 
 * @throws ParserConfigurationException
 */
  @Test
  public void testQueueConfigurationParser()
      throws ParserConfigurationException {
    JobQueueInfo info = new JobQueueInfo("root", "rootInfo");
    JobQueueInfo infoChild1 = new JobQueueInfo("child1", "child1Info");
    JobQueueInfo infoChild2 = new JobQueueInfo("child2", "child1Info");

    info.addChild(infoChild1);
    info.addChild(infoChild2);
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
        .newInstance();
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document document = builder.newDocument();
    Element e = QueueConfigurationParser.getQueueElement(document, info);
    DOMImplementationImpl domImplLS = (DOMImplementationImpl) document
        .getImplementation();
    LSSerializer serializer = domImplLS.createLSSerializer();
    // write to String for check 
    String str = serializer.writeToString(e);
    assertTrue(str
        .endsWith("<queue><name>root</name><properties/><state>running</state><queue><name>child1</name><properties/><state>running</state></queue><queue><name>child2</name><properties/><state>running</state></queue></queue>"));
  }
}
