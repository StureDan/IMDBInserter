using Microsoft.Data.SqlClient;
using System;

class Program
{
    const string connectionString = "Server=(localdb)\\MSSQLLocalDB;Database=IMDB3;Integrated Security=True;TrustServerCertificate=True;";

    static void Main()
    {
        while (true)
        {
            Console.Clear();
            Console.WriteLine("==== IMDB Console ====");
            Console.WriteLine("1) Search Movies");
            Console.WriteLine("2) Search Persons");
            Console.WriteLine("3) Add Movie");
            Console.WriteLine("4) Add Person");
            Console.WriteLine("5) Update Movie");
            Console.WriteLine("6) Delete Movie");
            Console.WriteLine("0) Exit");
            Console.Write("Choose: ");
            string choice = Console.ReadLine();

            switch (choice)
            {
                case "1": SearchMovies(); break;
                case "2": SearchPersons(); break;
                case "3": AddMovieSimple(); break;
                case "4": AddPerson(); break;
                case "5": UpdateMovie(); break;
                case "6": DeleteMovie(); break;
                case "0": return;
                default: Console.WriteLine("Invalid choice"); break;
            }

            Console.WriteLine("\nPress any key to continue...");
            Console.ReadKey();
        }
    }

    static void SearchMovies()
    {
        Console.Write("Enter movie search term: ");
        string search = Console.ReadLine();

        using var conn = new SqlConnection(connectionString);
        conn.Open();

        using var cmd = new SqlCommand("SearchMovies", conn);
        cmd.CommandType = System.Data.CommandType.StoredProcedure;
        cmd.Parameters.AddWithValue("@Search", search);

        using var reader = cmd.ExecuteReader();
        Console.WriteLine("\n--- Results ---");
        while (reader.Read())
            Console.WriteLine($"{reader["Id"]}: {reader["PrimaryTitle"]}");
    }

    static void SearchPersons()
    {
        Console.Write("Enter person search term: ");
        string search = Console.ReadLine();

        using var conn = new SqlConnection(connectionString);
        conn.Open();

        using var cmd = new SqlCommand("SearchPersons", conn);
        cmd.CommandType = System.Data.CommandType.StoredProcedure;
        cmd.Parameters.AddWithValue("@Search", search);

        using var reader = cmd.ExecuteReader();
        Console.WriteLine("\n--- Results ---");
        while (reader.Read())
            Console.WriteLine($"{reader["Id"]}: {reader["Name"]}");
    }

    static void AddMovieSimple()
    {
        Console.WriteLine("\n--- Add New Movie (C# handles ID) ---");
        Console.Write("Movie title: ");
        string title = Console.ReadLine();

        Console.Write("Original title (optional): ");
        string original = Console.ReadLine();
        // Brug null hvis strengen er tom eller whitespace
        string originalOrNull = String.IsNullOrWhiteSpace(original) ? null : original;

        // Hardcodet værdier for de øvrige kolonner
        int titleTypeId = 1;
        int isAdult = 0;

        using var conn = new SqlConnection(connectionString);
        conn.Open();

        int newId = 0;

        // STEP 1: Find det næste ledige ID (Manuel ID-generering)
        // Dette er nødvendigt, da vi ikke lader databasen auto-generere Id
        try
        {
            using (var idCmd = new SqlCommand("SELECT MAX(Id) FROM Titles", conn))
            {
                var result = idCmd.ExecuteScalar();
                if (result != DBNull.Value && result != null)
                {
                    int maxId = Convert.ToInt32(result);
                    newId = maxId + 1;
                }
                else
                {
                    // Hvis tabellen er tom, start ved 1
                    newId = 1;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error finding max Id: {ex.Message}");
            return; // Afslut funktionen hvis ID ikke kan findes
        }

        // STEP 2: Opsæt kommandoen MED @Id parameteren
        using var cmd = new SqlCommand("AddMovieSimple", conn);
        cmd.CommandType = System.Data.CommandType.StoredProcedure;

        // NYT: Inkluder det manuelt beregnede Id
        cmd.Parameters.AddWithValue("@Id", newId);

        // Alle øvrige parametre
        cmd.Parameters.AddWithValue("@TitleTypeId", titleTypeId);
        cmd.Parameters.AddWithValue("@PrimaryTitle", title);

        cmd.Parameters.AddWithValue("@OriginalTitle", (object?)originalOrNull ?? DBNull.Value);

        cmd.Parameters.AddWithValue("@IsAdult", isAdult);

        // Sæt de valgfrie int-felter til DBNull.Value
        cmd.Parameters.AddWithValue("@StartYear", DBNull.Value);
        cmd.Parameters.AddWithValue("@EndYear", DBNull.Value);
        cmd.Parameters.AddWithValue("@RuntimeMinutes", DBNull.Value);

        try
        {
            // STEP 3: Udfør indsættelsen
            cmd.ExecuteNonQuery();
            Console.WriteLine($"✅ Movie added successfully. C# assigned ID: {newId}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error adding movie: {ex.Message}");
        }
    }
    static void AddPerson()
    {
        Console.WriteLine("\n--- Add New Person (C# handles ID) ---");

        // Indsaml data
        Console.Write("Person name: ");
        string name = Console.ReadLine();

        Console.Write("Birth Year (optional, e.g., 1980): ");
        string birthYearInput = Console.ReadLine();

        Console.Write("Death Year (optional, e.g., 2020): ");
        string deathYearInput = Console.ReadLine();

        // Håndter valgfrie Int-felter
        object birthYearParam = int.TryParse(birthYearInput, out int birthYear) ? (object)birthYear : DBNull.Value;
        object deathYearParam = int.TryParse(deathYearInput, out int deathYear) ? (object)deathYear : DBNull.Value;

        using var conn = new SqlConnection(connectionString);
        conn.Open();

        int newId = 0;

        // STEP 1: Find det næste ledige ID fra Persons-tabellen
        try
        {
            using (var idCmd = new SqlCommand("SELECT MAX(Id) FROM Persons", conn))
            {
                var result = idCmd.ExecuteScalar();
                if (result != DBNull.Value && result != null)
                {
                    int maxId = Convert.ToInt32(result);
                    newId = maxId + 1;
                }
                else
                {
                    // Hvis tabellen er tom, start ved 1
                    newId = 1;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error finding max Id for Persons: {ex.Message}");
            return;
        }

        // STEP 2: Opsæt kommandoen MED @Id parameteren
        using var cmd = new SqlCommand("AddPerson", conn);
        cmd.CommandType = System.Data.CommandType.StoredProcedure;

        // Inkluder det manuelt beregnede Id
        cmd.Parameters.AddWithValue("@Id", newId);

        // Alle øvrige parametre
        cmd.Parameters.AddWithValue("@Name", name);
        cmd.Parameters.AddWithValue("@BirthYear", birthYearParam);
        cmd.Parameters.AddWithValue("@DeathYear", deathYearParam);

        try
        {
            // STEP 3: Udfør indsættelsen
            cmd.ExecuteNonQuery();
            Console.WriteLine($"✅ Person added successfully. C# assigned ID: {newId}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error adding person: {ex.Message}");
        }
    }
    static void UpdateMovie()
    {
        Console.Write("Movie Id to update: ");
        int id = int.Parse(Console.ReadLine());

        Console.Write("New Title (blank to keep): ");
        string title = Console.ReadLine();
        if (string.IsNullOrWhiteSpace(title)) title = null;

        Console.Write("New Runtime (blank to keep): ");
        string runtimeInput = Console.ReadLine();
        int? runtime = string.IsNullOrWhiteSpace(runtimeInput) ? null : int.Parse(runtimeInput);

        using var conn = new SqlConnection(connectionString);
        conn.Open();

        using var cmd = new SqlCommand("UpdateMovieBasic", conn);
        cmd.CommandType = System.Data.CommandType.StoredProcedure;

        cmd.Parameters.AddWithValue("@Id", id);
        cmd.Parameters.AddWithValue("@PrimaryTitle", (object?)title ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@RuntimeMinutes", (object?)runtime ?? DBNull.Value);

        // allow null flags
        cmd.Parameters.AddWithValue("@SetNull_OriginalTitle", 0);
        cmd.Parameters.AddWithValue("@SetNull_StartYear", 0);
        cmd.Parameters.AddWithValue("@SetNull_EndYear", 0);
        cmd.Parameters.AddWithValue("@SetNull_RuntimeMinutes", runtime == null ? 1 : 0);

        cmd.ExecuteNonQuery();

        Console.WriteLine("✅ Movie updated");
    }

    static void DeleteMovie()
    {
        Console.Write("Movie Id to delete: ");
        int id = int.Parse(Console.ReadLine());

        using var conn = new SqlConnection(connectionString);
        conn.Open();

        using var cmd = new SqlCommand("DeleteMovie", conn);
        cmd.CommandType = System.Data.CommandType.StoredProcedure;
        cmd.Parameters.AddWithValue("@Id", id);

        cmd.ExecuteNonQuery();

        Console.WriteLine("✅ Movie deleted");
    }
}
