using IMDBInserter;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

string connectionString =
    "Server=localhost;Database=IMDB2;Integrated Security=True;TrustServerCertificate=True;";

using SqlConnection sqlConn = new SqlConnection(connectionString);
sqlConn.Open();

using SqlTransaction sqlTrans = sqlConn.BeginTransaction();

try
{
    string filename = @"C:\Users\sture\OneDrive\Skrivebord\zippydippy\title.basics.tsv";

    IEnumerable<string> imdbData = File.ReadAllLines(filename)
                                       .Skip(1)       // skip header
                                       .Take(10000);  // just in case
                                                      // læg det her før foreach-løkken:
    var titleTypeIds = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
    int nextTypeId = 1;
    var genreIds = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
    int nextGenreId = 1;

    foreach (string titleString in imdbData)
    {
        string[] values = titleString.Split('\t');
        if (values.Length == 9)
        {
            try
            {
                var typeName = values[1]; // "movie", "short", "tvEpisode" osv.

                if (!titleTypeIds.TryGetValue(typeName, out int typeId))
                {
                    typeId = nextTypeId++;
                    titleTypeIds[typeName] = typeId;

                    // valgfrit: indsæt TitleType i databasen
                    using var insertType = new SqlCommand(
                        "INSERT INTO TitleTypes (Id, Name) VALUES (@id, @name)", sqlConn, sqlTrans);
                    insertType.Parameters.AddWithValue("@id", typeId);
                    insertType.Parameters.AddWithValue("@name", typeName);
                    insertType.ExecuteNonQuery();
                }

                // genres
                var genres = values[8] == "\\N" ? new List<string>() : values[8].Split(',').ToList();
                foreach (var genre in genres)
                {
                    if (!genreIds.ContainsKey(genre))
                    {
                        int genreId = nextGenreId++;
                        genreIds[genre] = genreId;

                        // valgfrit: indsæt Genre i databasen
                        using var insertGenre = new SqlCommand(
                                                       "INSERT INTO Genres (Id, Name) VALUES (@id, @name)", sqlConn, sqlTrans);
                        insertGenre.Parameters.AddWithValue("@id", genreId);
                        insertGenre.Parameters.AddWithValue("@name", genre);
                        insertGenre.ExecuteNonQuery();
                    }
                }

                var title = new Title
                {
                    Id = int.Parse(values[0].Substring(2)),
                    TitleTypeId = typeId,   // <-- FK fra dictionary
                    PrimaryTitle = values[2],
                    OriginalTitle = values[3] == "\\N" ? null : values[3],
                    IsAdult = values[4] == "1",
                    StartYear = values[5] == "\\N" ? (int?)null : int.Parse(values[5]),
                    EndYear = values[6] == "\\N" ? (int?)null : int.Parse(values[6]),
                    RuntimeMinutes = values[7] == "\\N" ? (int?)null : int.Parse(values[7]),
                };


                string sql = title.ToSql(); // bygger INSERT

                using SqlCommand cmd = new SqlCommand(sql, sqlConn, sqlTrans);
                //cmd.ExecuteNonQuery();
                
            }
            catch (Exception exRow)
            {
                Console.WriteLine("Error parsing/inserting line:");
                Console.WriteLine(titleString);
                Console.WriteLine(exRow.Message);
            }
        }
        else
        {
            Console.WriteLine("Not 9 values: " + titleString);
        }
    }

    sqlTrans.Commit();
    Console.WriteLine("Import OK ✅");
}
catch (Exception ex)
{
    Console.WriteLine("Critical error, rolling back ❌");
    Console.WriteLine(ex.Message);
    try { sqlTrans.Rollback(); } catch { /* ignore rollback exceptions */ }
}
finally
{
    sqlConn.Close();
}
