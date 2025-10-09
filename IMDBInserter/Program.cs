using IMDBInserter;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Text;

// --- Configuration ---
const string connectionString = "Server=localhost;Database=IMDB2;Integrated Security=True;TrustServerCertificate=True;";

// 🎯 CHOOSE WHICH FILE TO PROCESS 🎯
const string fileToImportTitles = @"C:\temp\Database obl\title.basics.tsv.gz";
const string fileToImportPersons = @"C:\temp\Database obl\name.basics.tsv.gz";
const string fileToImportCrew = @"C:\temp\Database obl\title.crew.tsv.gz";

// 🎯 TOGGLE THIS BOOLEAN TO SWITCH BETWEEN MODES 🎯
const bool IsDryRun = false; // Set to 'false' for a full database run

// --- Main Program Execution ---
Console.WriteLine("Starting import process...");
try
{
    
    ImportTitles(connectionString, fileToImportTitles, IsDryRun);
    ImportPersons(connectionString, fileToImportPersons, IsDryRun);
    ImportCrew(connectionString, fileToImportCrew, IsDryRun); 
}
catch (FileNotFoundException ex)
{
    Console.WriteLine($"Error: File not found. {ex.Message} ❌");
}
catch (Exception ex)
{
    Console.WriteLine($"A critical error occurred: {ex.Message} ❌");
}
Console.WriteLine("Import process finished.");


/// <summary>
/// Reads name.basics.tsv.gz and bulk inserts person data.
/// </summary>
static void ImportPersons(string connStr, string filename, bool dryRun)
{
    Console.WriteLine("\n--- Starting Person Import ---");
    int maxRows = dryRun ? 10000 : 500000;
    int rowsProcessed = 0;

    // Data structure definitions...
    var personsTable = new DataTable("Persons");
    personsTable.Columns.Add("Id", typeof(int));
    personsTable.Columns.Add("Name", typeof(string));
    personsTable.Columns.Add("BirthYear", typeof(int)) 
    ;
    personsTable.Columns.Add("DeathYear", typeof(int)) 
    ;

    var professionsTable = new DataTable("Professions");
    professionsTable.Columns.Add("Id", typeof(int));
    professionsTable.Columns.Add("Name", typeof(string));

    var linkPersonProfessionTable = new DataTable("LinkPersonProfession");
    linkPersonProfessionTable.Columns.Add("PersonId", typeof(int));
    linkPersonProfessionTable.Columns.Add("ProfessionId", typeof(int));

    // Omdøbt for konsistens, da den linker en person til en titel (KnownFor)
    var knownForTable = new DataTable("KnownFor");
    knownForTable.Columns.Add("PersonId", typeof(int));
    knownForTable.Columns.Add("TitleId", typeof(int));

    var professionIds = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
    int nextProfessionId = 1;

    // ... (file reading setup remains the same) ...
    using var fileStream = new FileStream(filename, FileMode.Open, FileAccess.Read);
    using var gzipStream = new GZipStream(fileStream, CompressionMode.Decompress);
    using var reader = new StreamReader(gzipStream, Encoding.UTF8);
    reader.ReadLine(); // Skip header

    string currentLine;
    while ((currentLine = reader.ReadLine()) != null && rowsProcessed < maxRows)
    {
        var values = currentLine.Split('\t');
        if (values.Length != 6) continue;

        try
        {
            if (!int.TryParse(values[0].AsSpan(2), out int personId)) continue;

            // --- Profession Processing (uændret) ---
            var professionNames = values[4].Split(',');
            foreach (var professionName in professionNames)
            {
                if (professionName == @"\N" || string.IsNullOrWhiteSpace(professionName)) continue;
                if (!professionIds.TryGetValue(professionName, out int currentProfessionId))
                {
                    currentProfessionId = nextProfessionId++;
                    professionIds[professionName] = currentProfessionId;
                    professionsTable.Rows.Add(currentProfessionId, professionName);
                }
                linkPersonProfessionTable.Rows.Add(personId, currentProfessionId);
            }

            
            var knownForTitles = values[5].Split(','); 
            foreach (var titleConst in knownForTitles)
            {
                if (titleConst == @"\N" || string.IsNullOrWhiteSpace(titleConst)) continue;

                // Parser title ID (f.eks., "tt0111161" -> 111161)
                if (int.TryParse(titleConst.AsSpan(2), out int titleId))
                {
                    // Tilføjer linket til den nye tabel
                    knownForTable.Rows.Add(personId, titleId);
                }
            }
           

            // --- Person Processing (uændret) ---
            var personRow = personsTable.NewRow();
            personRow["Id"] = personId;
            personRow["Name"] = values[1];
            personRow["BirthYear"] = values[2] == @"\N" ? DBNull.Value : int.Parse(values[2]);
            personRow["DeathYear"] = values[3] == @"\N" ? DBNull.Value : int.Parse(values[3]);
            personsTable.Rows.Add(personRow);

            rowsProcessed++;
        }
        catch (Exception exRow)
        {
            Console.WriteLine($"Error parsing person row: {currentLine}\n\t{exRow.Message}");
        }
    }

    if (dryRun)
    {
        Console.WriteLine("\n--- Person Dry Run Complete --- ✅");
        Console.WriteLine($"📝 Would insert {personsTable.Rows.Count:N0} persons.");
        Console.WriteLine($"📝 Would insert {professionsTable.Rows.Count:N0} new professions.");
        Console.WriteLine($"📝 Would create {linkPersonProfessionTable.Rows.Count:N0} person-profession links.");
        // Tilføjet opsummering for den nye tabel
        Console.WriteLine($"📝 Would create {knownForTable.Rows.Count:N0} 'Known For' title links.");
    }
    else
    {
        using var conn = new SqlConnection(connStr);
        conn.Open();
        using var trans = conn.BeginTransaction();
        try
        {
            BulkInsert(personsTable, "Persons", conn, trans);
            BulkInsert(professionsTable, "Professions", conn, trans);
            BulkInsert(linkPersonProfessionTable, "LinkPersonProfession", conn, trans);
            // Tilføjet BulkInsert for den nye tabel
            BulkInsert(knownForTable, "KnownFor", conn, trans);

            trans.Commit();
            Console.WriteLine($"\nPerson import complete! {rowsProcessed:N0} rows successfully inserted. ✅");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Database error during person import. Rolling back. ❌\n\t{ex.Message}");
            trans.Rollback();
            throw;
        }
    }
}
/// <summary>
/// Reads title.basics.tsv.gz and bulk inserts title, genre, and type data.
/// </summary>
static void ImportTitles(string connStr, string filename, bool dryRun)
{
    Console.WriteLine("\n--- Starting Title Import ---");
     int maxRows = dryRun ? 10000 : 500000;
    int rowsProcessed = 0;

    // DataTables for titles and related data
    var titlesTable = new DataTable("Titles");
    // ... (DataTable definitions are the same as your original code) ...
    titlesTable.Columns.Add("Id", typeof(int));
    titlesTable.Columns.Add("TitleTypeId", typeof(int));
    titlesTable.Columns.Add("PrimaryTitle", typeof(string));
    titlesTable.Columns.Add("OriginalTitle", typeof(string));
    titlesTable.Columns.Add("IsAdult", typeof(bool));
    titlesTable.Columns.Add("StartYear", typeof(int)) 
    ;
    titlesTable.Columns.Add("EndYear", typeof(int))
    ;
    titlesTable.Columns.Add("RuntimeMinutes", typeof(int)) 
    ;

    var titleTypesTable = new DataTable("TitleTypes");
    titleTypesTable.Columns.Add("Id", typeof(int));
    titleTypesTable.Columns.Add("Name", typeof(string));

    var genresTable = new DataTable("Genres");
    genresTable.Columns.Add("Id", typeof(int));
    genresTable.Columns.Add("Name", typeof(string));

    var titleGenresTable = new DataTable("TitleGenres");
    titleGenresTable.Columns.Add("TitleId", typeof(int));
    titleGenresTable.Columns.Add("GenreId", typeof(int));

    // In-memory lookups
    var titleTypeIds = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
    var genreIds = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
    int nextTypeId = 1, nextGenreId = 1;

    using var fileStream = new FileStream(filename, FileMode.Open, FileAccess.Read);
    using var gzipStream = new GZipStream(fileStream, CompressionMode.Decompress);
    using var reader = new StreamReader(gzipStream, Encoding.UTF8);

    reader.ReadLine(); // Skip header

    string currentLine;
    while ((currentLine = reader.ReadLine()) != null && rowsProcessed < maxRows)
    {
        // ... (The entire while loop logic from your original code goes here) ...
        var values = currentLine.Split('\t');
        if (values.Length != 9) continue;
        try
        {
            if (!int.TryParse(values[0].AsSpan(2), out int titleId)) continue;
            var typeName = values[1];
            if (!titleTypeIds.TryGetValue(typeName, out int typeId))
            {
                typeId = nextTypeId++;
                titleTypeIds[typeName] = typeId;
                titleTypesTable.Rows.Add(typeId, typeName);
            }
            var genreNames = values[8].Split(',');
            foreach (var genreName in genreNames)
            {
                if (genreName == @"\N" || string.IsNullOrWhiteSpace(genreName)) continue;
                if (!genreIds.TryGetValue(genreName, out int genreId))
                {
                    genreId = nextGenreId++;
                    genreIds[genreName] = genreId;
                    genresTable.Rows.Add(genreId, genreName);
                }
                titleGenresTable.Rows.Add(titleId, genreId);
            }
            var titleRow = titlesTable.NewRow();
            titleRow["Id"] = titleId;
            titleRow["TitleTypeId"] = typeId;
            titleRow["PrimaryTitle"] = values[2];
            titleRow["OriginalTitle"] = values[3] == @"\N" ? DBNull.Value : values[3];
            titleRow["IsAdult"] = values[4] == "1";
            titleRow["StartYear"] = values[5] == @"\N" ? DBNull.Value : int.Parse(values[5]);
            titleRow["EndYear"] = values[6] == @"\N" ? DBNull.Value : int.Parse(values[6]);
            titleRow["RuntimeMinutes"] = values[7] == @"\N" ? DBNull.Value : int.Parse(values[7]);
            titlesTable.Rows.Add(titleRow);
            rowsProcessed++;
        }
        catch (Exception exRow)
        {
            Console.WriteLine($"Error parsing title row: {currentLine}\n\t{exRow.Message}");
        }
    }

    if (dryRun)
    {
        Console.WriteLine("\n--- Title Dry Run Complete --- ✅");
        Console.WriteLine($"\t- Would insert {titlesTable.Rows.Count:N0} Titles");
        Console.WriteLine($"\t- Would insert {titleTypesTable.Rows.Count:N0} new Title Types");
        Console.WriteLine($"\t- Would insert {genresTable.Rows.Count:N0} new Genres");
        Console.WriteLine($"\t- Would insert {titleGenresTable.Rows.Count:N0} Title-Genre relationships");
        
        
    }
    else
    {
        using var conn = new SqlConnection(connStr);
        conn.Open();
        using var trans = conn.BeginTransaction();
        try
        {
            BulkInsert(titleTypesTable, "TitleTypes", conn, trans);
            BulkInsert(genresTable, "Genres", conn, trans);
            BulkInsert(titlesTable, "Titles", conn, trans);
            BulkInsert(titleGenresTable, "TitleGenres", conn, trans);
            trans.Commit();
            Console.WriteLine($"\nTitle import complete! {rowsProcessed:N0} rows successfully inserted. ✅");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Database error during title import. Rolling back. ❌\n\t{ex.Message}");
            trans.Rollback();
            throw;
        }
    }
}
/// <summary>
/// Reads title.crew.tsv.gz and bulk inserts director and writer links.
/// </summary>
static void ImportCrew(string connStr, string filename, bool dryRun)
{
    Console.WriteLine("\n--- Starting Crew Import (Directors & Writers) ---");
    int maxRows = dryRun ? 10000 : 500000;
    int rowsProcessed = 0;

    // DataTables to hold the links between titles and persons
    var linkTitleDirectorTable = new DataTable("LinkTitleDirector");
    linkTitleDirectorTable.Columns.Add("TitleId", typeof(int));
    linkTitleDirectorTable.Columns.Add("PersonId", typeof(int));

    var linkTitleWriterTable = new DataTable("LinkTitleWriter");
    linkTitleWriterTable.Columns.Add("TitleId", typeof(int));
    linkTitleWriterTable.Columns.Add("PersonId", typeof(int));

    using var fileStream = new FileStream(filename, FileMode.Open, FileAccess.Read);
    using var gzipStream = new GZipStream(fileStream, CompressionMode.Decompress);
    using var reader = new StreamReader(gzipStream, Encoding.UTF8);

    reader.ReadLine(); // Skip header

    string currentLine;
    while ((currentLine = reader.ReadLine()) != null && rowsProcessed < maxRows)
    {
        var values = currentLine.Split('\t');
        // The crew file has 3 columns: tconst, directors, writers
        if (values.Length != 3) continue;

        try
        {
            // --- Parse Title ID ---
            if (!int.TryParse(values[0].AsSpan(2), out int titleId)) continue;

            // --- Process Directors (values[1]) ---
            var directorIds = values[1].Split(',');
            foreach (var directorConst in directorIds)
            {
                if (directorConst == @"\N" || string.IsNullOrWhiteSpace(directorConst)) continue;

                // Parse person ID (e.g., "nm0005690" -> 5690)
                if (int.TryParse(directorConst.AsSpan(2), out int personId))
                {
                    linkTitleDirectorTable.Rows.Add(titleId, personId);
                }
            }

            // --- Process Writers (values[2]) ---
            var writerIds = values[2].Split(',');
            foreach (var writerConst in writerIds)
            {
                if (writerConst == @"\N" || string.IsNullOrWhiteSpace(writerConst)) continue;

                // Parse person ID
                if (int.TryParse(writerConst.AsSpan(2), out int personId))
                {
                    linkTitleWriterTable.Rows.Add(titleId, personId);
                }
            }

            rowsProcessed++;
        }
        catch (Exception exRow)
        {
            Console.WriteLine($"Error parsing crew row: {currentLine}\n\t{exRow.Message}");
        }
    }

    if (dryRun)
    {
        Console.WriteLine("\n--- Crew Dry Run Complete --- ✅");
        Console.WriteLine($"\t- Would create {linkTitleDirectorTable.Rows.Count:N0} director links.");
        Console.WriteLine($"\t- Would create {linkTitleWriterTable.Rows.Count:N0} writer links.");
    }
    else
    {
        using var conn = new SqlConnection(connStr);
        conn.Open();
        using var trans = conn.BeginTransaction();
        try
        {
            BulkInsert(linkTitleDirectorTable, "LinkTitleDirector", conn, trans);
            BulkInsert(linkTitleWriterTable, "LinkTitleWriter", conn, trans);

            trans.Commit();
            Console.WriteLine($"\nCrew import complete! Links for {rowsProcessed:N0} titles processed. ✅");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Database error during crew import. Rolling back. ❌\n\t{ex.Message}");
            trans.Rollback();
            throw;
        }
    }
}
/// <summary>
/// Helper method to perform a SqlBulkCopy operation.
/// </summary>
static void BulkInsert(DataTable table, string destinationTable, SqlConnection conn, SqlTransaction trans)
{
    if (table.Rows.Count == 0)
    {
        Console.WriteLine($"Skipping bulk insert for '{destinationTable}' as there is no data.");
        return;
    }

    using var bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, trans);
    bulkCopy.DestinationTableName = destinationTable;
    foreach (DataColumn col in table.Columns)
    {
        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
    }

    bulkCopy.WriteToServer(table);
    Console.WriteLine($"Successfully inserted {table.Rows.Count:N0} rows into '{destinationTable}'.");
}