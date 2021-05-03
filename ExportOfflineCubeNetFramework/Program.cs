using Microsoft.AnalysisServices;
using Microsoft.AnalysisServices.AdomdClient;
using System;
using System.IO;
using System.Xml;

namespace ExportOfflineCubeNetFramework
{
    class Program
    {
        static void Main(string[] args)
        {
            
            string server = "MAXIMEGAGNE96DA\\MSSQLSSAS";
            string dbName = "MySecondCube";
            string cubeName = "MyCube";
            string fileName = "C:\\Users\\maximegagne\\Documents\\OfflineCube\\";
            try
            {
                //if (args.Length != 4)
                //{
                //    throw new Exception("The program requires 4 argumens: SSAS Server name, SSAS database name, cube name, offline cube file name (full path) ");
                //}
                //string server = args[0];
                //string dbName = args[1];
                //string cubeName = args[2];
                //string fileName = args[3];

                if(parameterValidation(server, dbName, cubeName, fileName))
                {
                    Console.WriteLine(string.Format("Creating Offline cube for [{0}]/[{1}]", dbName, cubeName));
                    GenerateofflineCube(server, dbName, cubeName, fileName, true);
                }
            }

            catch (Exception ex)
            {
                Console.WriteLine("Generating offline cube failed:");
                Console.WriteLine(ex.Message);
                throw ex;
            }
        }

        // This method verify if all parameter are correct
        public static bool parameterValidation(string server, string dbName, string cubeName, string path)
        {
            return true;
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