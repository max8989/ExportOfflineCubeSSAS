using Microsoft.AnalysisServices;
using Microsoft.AnalysisServices.AdomdClient;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;


namespace ExportOfflineCubeNetFramework
{
    class Program
    {
        static void Main(string[] args)
        {
            foreach (var item in args)
            {
                Console.WriteLine(item);
            }
            try
            {
                var param = paramsInit(args);

                Console.WriteLine(string.Format("\n Creating Offline cube for [{0}]/[{1}]", param["dbName"], param["cubeName"]));
                GenerateofflineCube(param["server"], param["dbName"],param["cubeName"],param["fileName"], true);
            }

            catch (Exception ex)
            {
                Console.WriteLine("Generating offline cube failed:");
                Console.WriteLine(ex.Message);
                throw ex;
            }
        }

        // This method initialize parmas from args, json file or defauld config
        public static Dictionary<string, string> paramsInit(string[] args)
        {
            var paramValues = new Dictionary<string, string>();
            // Params from agrs
            if (args.Length == 4)
            {
                paramValues["server"] = args[0];
                paramValues["dbName"] = args[1];
                paramValues["cubeName"] = args[2];
                paramValues["fileName"] = args[3];
                Console.WriteLine("Parameter from agruments are being used.");
                return paramValues;
            }

            // Params from JSON file
            if (File.Exists("CubeParams.json"))
            {
                paramValues = deSerializeFromJson("CubeParams.json");
                if (paramValues["cubeName"] != null && paramValues["fileName"] != null && paramValues["dbName"] != null && paramValues["server"] != null)
                {
                    Console.WriteLine("Parameter from JSON file are being used.");
                    return paramValues;
                }
            }


            // Params from default config
            //var paramsFromInitialConfig = initialConfig();
            //if (paramsFromInitialConfig.cubeName != null && paramsFromInitialConfig.dbName != null && paramsFromInitialConfig.server != null && paramsFromInitialConfig.fileName != null)
            //{
            //    server = paramsFromInitialConfig.server;
            //    dbName = paramsFromInitialConfig.dbName;
            //    cubeName = paramsFromInitialConfig.cubeName;
            //    path = paramsFromInitialConfig.fileName;
            //    serializeToJSON(paramsFromInitialConfig);
            //    Console.WriteLine("Parameter from initial config are being used and saved to 'CubeParams.json' .");
            //    return paramsFromInitialConfig;
            //}

            // Test params
            Console.WriteLine("Tests parameter are being used.");
            paramValues["server"] = @"MAXIMEGAGNE96DA\MSSQLSSAS";
            paramValues["dbName"] = "MyPracticeCube";
            paramValues["cubeName"] = "MyPracticeCube";
            paramValues["fileName"] = @"C:\Users\maximegagne\Documents\OfflineCube\";
            serializeToJSON(paramValues);
            return paramValues;         
        }



        // This method verify if all parameter are correct
        public static Dictionary<string, string> initialConfig()
        {
            var param = new Dictionary<string, string>();
            bool isRunning;

            do
            {
                isRunning = true;
                if (param["server"] is null)
                {
                    Console.WriteLine("Please enter the server name");
                    param["server"] = Console.ReadLine();
                    Console.WriteLine("The server name you entered: {0}", param["server"]);
                    Console.WriteLine();
                }

                if (param["dbName"] is null)
                {
                    Console.WriteLine("Please enter the database name");
                    param["dbName"] = Console.ReadLine();
                    Console.WriteLine("The database name you entered: {0}", param["dbName"]);
                    Console.WriteLine();
                }

                if (param["cubeName"] is null)
                {
                    Console.WriteLine("Please enter the cube name");
                    param["cubeName"] = Console.ReadLine();
                    Console.WriteLine("The cube name you entered: {0}", param["cubeName"]);
                    Console.WriteLine();
                }

                if (param["fileName"] is null)
                {
                    Console.WriteLine("Please enter the path for the file");
                    param["fileName"] = Console.ReadLine();
                    Console.WriteLine("The path you entered: {0}", param["fileName"]);
                    Console.WriteLine();
                }

                if (param["server"] != null && param["dbName"] != null && param["cubeName"] != null && param["fileName"] != null)
                {
                    try
                    {
                        //AdomdConnection con = new AdomdConnection();
                        Server s = new Server();

                        s.Connect(param["server"]);

                        var dbObj = s.Databases.FindByName(param["dbName"]);
                        if (dbObj == null) 
                        {
                            isRunning = true;
                            string.Format("Database not found: {0}", param["dbName"]);
                            param["dbName"] = null;
                        }
                        var cubeObj = dbObj.Cubes.FindByName(param["cubeName"]);
                        if (cubeObj == null) 
                        {
                            isRunning = true;
                            string.Format("Cube not found: {0}", param["cubeName"]);
                            param["cubeName"] = null;
                        }
                        isRunning = false;
                    }
                    catch (Exception e)
                    {
                        isRunning = true;
                        Console.WriteLine("Error: " + e);
                        Console.WriteLine("Try again");
                    }

                }

            } while (isRunning);

            return param;
        }



        // This method Serialize to JSON File
        public static bool serializeToJSON(Dictionary<string, string> param)
        {
            try
            {
                // serialize JSON to a string and then write string to a file
                var cubeParams = JsonConvert.SerializeObject(param);

                // serialize JSON directly to a file
                File.WriteAllText($"{param["dbName"]}_{param["cubeName"]}_Offline.json", cubeParams);
                Console.WriteLine("Initial Parameter saved to CubeParams.json");
                return true;
            }
            catch (Exception)
            {
                throw;
            }
        }

        // This method check if the json file contains data and DeSerialize them
        public static Dictionary<string, string> deSerializeFromJson(string jsonCube)
        {
            string json = File.ReadAllText(jsonCube);
            var cube = JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
            return cube;
        }

        public static List<string> GetAllJsonFile()
        {
            return new List<string>() { "" };
        }


        // This method generate an offline Cube
        public static bool GenerateofflineCube(string server, string dbName, string cubeName, string path, bool removeTranslations)
        {
            bool result = false;
            var fileName = Path.GetFileNameWithoutExtension(path);
            var dir = Path.GetDirectoryName(path);
            var ext = Path.GetExtension(path);

            String dateFileName = string.Format("{0}_{1}", fileName, DateTime.Now.ToString("yyyyMMdd_HHmm"));
            string xmlaFile = Path.Combine(dir, dateFileName + ".xmla");
            string cubFile = Path.Combine(dir, dateFileName + ".cub");
            AdomdConnection con = new AdomdConnection();
            Server s = new Server();
            //Server s2 = new Server();
            try
            {
                s.Connect(server);
                var dbObj = s.Databases.FindByName(dbName);
                if (dbObj == null) throw new Exception(string.Format("Database not fount: {0}", dbName));
                var cubeObj = dbObj.Cubes.FindByName(cubeName);
                if (cubeObj == null) throw new Exception(string.Format("Cube not found: {0}", cubeName));

                MajorObject[] db = new MajorObject[1];
                db[0] = dbObj;

                if (File.Exists(cubFile)) File.Delete(cubFile);
                if (File.Exists(xmlaFile)) File.Delete(xmlaFile);

                // create an XML object to hold the xmla string
                XmlWriterSettings settings = new XmlWriterSettings();
                settings.Indent = true;
                settings.IndentChars = ("    ");
                settings.OmitXmlDeclaration = true;
                XmlWriter xmlWriter = XmlWriter.Create(xmlaFile, settings);

                // write the XML to a file
                Scripter scripter = new Scripter();
                scripter.ScriptCreate(db, xmlWriter, false);
                xmlWriter.Flush();
                xmlWriter.Close();

                String xmla = File.ReadAllText(xmlaFile);

                // get the xmla script into an XML document
                XmlDocument doc = new XmlDocument();
                doc.PreserveWhitespace = false;
                doc.LoadXml(xmla);

                // create the namespace so that we navigate the XML document
                XmlNamespaceManager nsmgr = new XmlNamespaceManager(doc.NameTable);
                nsmgr.AddNamespace("ms", "http://schemas.microsoft.com/analysisservices/2003/engine");

                // remove all the cube definitions that aren't the one we want
                XmlNodeList nodes = doc.SelectNodes("//ms:Cube", nsmgr);
                int nodesLeft = 0;
                for (int i = nodes.Count - 1; i >= 0; i--)
                {
                    if (nodes[i]["Name"].InnerText != cubeName)
                    {
                        { nodes[i].ParentNode.RemoveChild(nodes[i]); }
                    }
                    else
                    {
                        nodesLeft++;
                    }
                }

                if (nodesLeft != 1) throw new Exception(string.Format("All elements removed, cannot find the cube"));

                if (removeTranslations)
                {
                    XmlNodeList translationNodes = doc.SelectNodes("//ms:Translations", nsmgr);
                    foreach (XmlNode node in translationNodes)
                    {
                        node.RemoveAll();
                        //node.ParentNode.RemoveChild
                    }
                }

                // update the xmla variable with the new XML
                xmla = doc.OuterXml;

                // add the commands that will cause it to be processed
                xmla = @"<Batch xmlns=""http://schemas.microsoft.com/analysisservices/2003/engine"">" + xmla
                     + @"<Parallel>
                <Process xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:ddl2=""http://schemas.microsoft.com/analysisservices/2003/engine/2"" xmlns:ddl2_2=""http://schemas.microsoft.com/analysisservices/2003/engine/2/2"" xmlns:ddl100_100=""http://schemas.microsoft.com/analysisservices/2008/engine/100/100"" xmlns:ddl200=""http://schemas.microsoft.com/analysisservices/2010/engine/200"" xmlns:ddl200_200=""http://schemas.microsoft.com/analysisservices/2010/engine/200/200"">
                <Object>
                <DatabaseID>" + dbName + @"</DatabaseID>
                </Object>
                <Type>ProcessFull</Type>
                <WriteBackTableCreation>UseExisting</WriteBackTableCreation>
                </Process>
                </Parallel>
                </Batch>";
                // open an Adomd connection to a new CUB file
                //string cs = string.Format("Provider=MSOLAP;Data Source={0};Locale Identifier=1033", cubFile);

                // Create the file, or overwrite if the file exists.


                string cs = string.Format("Provider=MSOLAP;Data Source={0};Locale Identifier=1033", cubFile);


                //string cs = "Provider=MSOLAP;Data Source=njes1s6049:7033;Locale Identifier=1033";
                con.ConnectionString = cs;
                con.Open();
                // execute the XMLA against the CUB dump file
                AdomdCommand cmd = new AdomdCommand(xmla, con);
                cmd.Execute();
                //s2.Connect(cs);
                //s2.Execute(xmla);
                result = true;
            }
            catch (AdomdException e)
            {
                result = false;
                throw e;
            }
            finally
            {
                con.Close();
                s.Disconnect();
                // clean up
                if (File.Exists(xmlaFile)) File.Delete(xmlaFile);
            }
            Console.WriteLine(string.Format("Offline cube created: {0}", cubFile));

            Console.WriteLine("Press Enter to end!");
            var _ = Console.ReadLine();
            return result;   
        }

    }
}