using Microsoft.AnalysisServices;
using Microsoft.AnalysisServices.AdomdClient;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Timers;
using System.Xml;
using System.Diagnostics;


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

                //CreateJsonCubeConfig();

                GenerateAllOfflineCubeFromJson();
                Console.WriteLine("Press Enter to end!");
                var _ = Console.ReadLine();
            }

            catch (Exception ex)
            {
                Console.WriteLine("Generating offline cube failed:");
                Console.WriteLine(ex.Message);
                throw ex;
            }
        }

        public static bool CreateJsonCubeConfig()
        {
            try
            {
                var cubeParams = new Dictionary<string, string>();
                bool isRunning;

                do
                {
                    isRunning = true;
                    cubeParams["server"] = "";
                    cubeParams["dbName"] = "";
                    cubeParams["cubeName"] = "";
                    cubeParams["fileName"] = "";

                    Console.WriteLine("Type 'exit' to Aboard Operation\n");

                    if (cubeParams["server"] == "")
                    {
                        Console.Write("Please enter the server name: ");
                        cubeParams["server"] = Console.ReadLine();
                        if (cubeParams["server"].ToLower() == "exit") return false;
                        Console.WriteLine("The server name you entered: {0}", cubeParams["server"]);
                        Console.WriteLine();
                    }

                    if (cubeParams["dbName"] == "")
                    {
                        Console.Write("Please enter the database name: ");
                        cubeParams["dbName"] = Console.ReadLine();
                        if (cubeParams["dbName"].ToLower() == "exit") return false;
                        Console.WriteLine("The database name you entered: {0}", cubeParams["dbName"]);
                        Console.WriteLine();
                    }

                    if (cubeParams["cubeName"] == "")
                    {
                        Console.Write("Please enter the cube name: ");
                        cubeParams["cubeName"] = Console.ReadLine();
                        if (cubeParams["cubeName"].ToLower() == "exit") return false;
                        Console.WriteLine("The cube name you entered: {0}", cubeParams["cubeName"]);
                        Console.WriteLine();
                    }

                    if (cubeParams["fileName"] == "")
                    {
                        Console.Write("Please enter the path for the file: ");
                        cubeParams["fileName"] = Console.ReadLine();
                        if (cubeParams["fileName"].ToLower() == "exit") return false;
                        Console.WriteLine("The path you entered: {0}", cubeParams["fileName"]);
                        Console.WriteLine();
                    }

                    if (cubeParams["server"] != "" && cubeParams["dbName"] != "" && cubeParams["cubeName"] != "" && cubeParams["fileName"] != "")
                    {
                        try
                        {
                            Console.WriteLine("Testing Connection\n");
                            //AdomdConnection con = new AdomdConnection();
                            Server s = new Server();

                            s.Connect(cubeParams["server"]);

                            var dbObj = s.Databases.FindByName(cubeParams["dbName"]);
                            if (dbObj == null)
                            {
                                isRunning = true;
                                string.Format("Database not found: {0}", cubeParams["dbName"]);
                                cubeParams["dbName"] = null;
                            }
                            var cubeObj = dbObj.Cubes.FindByName(cubeParams["cubeName"]);
                            if (cubeObj == null)
                            {
                                isRunning = true;
                                string.Format("Cube not found: {0}", cubeParams["cubeName"]);
                                cubeParams["cubeName"] = null;
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

                SerializeToJSON(cubeParams);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message + "\n");
                return false;
                //throw ex;
            }
           
        }


        public static bool WriteLog(string strFileName, string strMessage)
        {
            try
            {
                FileStream objFilestream = new FileStream(string.Format("{0}\\{1}", Path.GetTempPath(), strFileName), FileMode.Append, FileAccess.Write);
                StreamWriter objStreamWriter = new StreamWriter((Stream)objFilestream);
                objStreamWriter.WriteLine(strMessage);
                objStreamWriter.Close();
                objFilestream.Close();
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }


        // This method Generate all offline cube from the JSON files
        public static void GenerateAllOfflineCubeFromJson()
        {
            var ErrorMessages = new List<string>();
            var JsonFiles = GetAllJsonFiles();
            
            

            if (JsonFiles.Count < 1)
            {
                Console.WriteLine("No Json files found");
                return;
            }

            var cubeParams = new Dictionary<string, string>();

            foreach (var jsonfile in JsonFiles)
            {
                try
                {
                    cubeParams = DeserializeFromJson(jsonfile);

                    Console.WriteLine(string.Format("\nCreating Offline cube for [{0}]/[{1}]",
                                        cubeParams["dbName"],
                                        cubeParams["cubeName"]));

                    GenerateofflineCube(cubeParams["server"],
                                        cubeParams["dbName"],
                                        cubeParams["cubeName"],
                                        cubeParams["fileName"],
                                        true);

                }
                catch (Exception ex)
                {
                    ErrorMessages.Add($"Json File: {jsonfile} ,\nError: {ex.Message} at {DateTime.Now}\n");
                    //throw ex;
                }
            }
            if(ErrorMessages.Count != 0)
            {
                Console.WriteLine("\n==================Error Messages==================");
                foreach (var errorMessage in ErrorMessages)
                {
                    Console.WriteLine(errorMessage);
                    WriteLog("OfflineCube.log", errorMessage);
                }
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
            //if (File.Exists("CubeParams.json"))
            //{
            //    paramValues = deSerializeFromJson("CubeParams.json");
            //    if (paramValues["cubeName"] != null && paramValues["fileName"] != null && paramValues["dbName"] != null && paramValues["server"] != null)
            //    {
            //        Console.WriteLine("Parameter from JSON file are being used.");
            //        return paramValues;
            //    }
            //}


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
            SerializeToJSON(paramValues);
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
        public static bool SerializeToJSON(Dictionary<string, string> param)
        {
            try
            {
                // serialize JSON to a string and then write string to a file
                var cubeParams = JsonConvert.SerializeObject(param);

                // serialize JSON directly to a file
                File.WriteAllText($"{param["dbName"]}_{param["cubeName"]}_Offline.json", cubeParams);
                Console.WriteLine($"Initial Parameter saved to {param["dbName"]}_{param["cubeName"]}_Offline.json");
                return true;
            }
            catch (Exception)
            {
                throw;
            }
        }

        // This method check if the json file contains data and DeSerialize them
        public static Dictionary<string, string> DeserializeFromJson(string jsonCube)
        {
            try
            {
                string json = File.ReadAllText(jsonCube);
                var cube = JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
                return cube;
            }
            catch (Exception e)
            {
                throw e;
            }
            
        }


        // This method generate an offline Cube
        public static bool GenerateofflineCube(string server, string dbName, string cubeName, string path, bool removeTranslations)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            bool result = false;
            var fileName = Path.GetFileNameWithoutExtension(path);
            var dir = Path.GetDirectoryName(path);
            var ext = Path.GetExtension(path);

            String dateFileName = string.Format("{0}_{1}", fileName, DateTime.Now.ToString("yyyyMMdd_HHmm"));
            string xmlaFile = Path.Combine(dir, dateFileName + ".xmla");
            string cubFile = Path.Combine(dir, dbName + "_" + cubeName + dateFileName + ".cub");
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
                Console.WriteLine($"Error cannot create cube for {dbName} {cubeName}, {e.Message} ");
                throw e;
            }
            finally
            {
                con.Close();
                s.Disconnect();
                // clean up
                if (File.Exists(xmlaFile)) File.Delete(xmlaFile);
                stopwatch.Stop();
               
            }
            Console.WriteLine(string.Format("Offline cube created: {0} at {1} in {2}\n",
                cubFile, DateTime.Now,stopwatch.Elapsed.ToString("hh\\:mm\\.ss")));

            WriteLog("OfflineCube.log", string.Format("Offline cube created: {0} at {1} in {2}\n",
                DateTime.Now, cubFile, stopwatch.Elapsed.ToString("hh\\:mm\\.ss")));
            
            return result;   
        }



        public static List<string> GetAllJsonFiles()
        {
            var list = new List<string>();
            DirectoryInfo d = new DirectoryInfo(".");//Assuming Test is your Folder
            FileInfo[] Files = d.GetFiles("*.json"); //Getting Text files
            foreach (FileInfo file in Files)
            {
                list.Add(file.Name);
            }

            return list;
        }


    }
}