using Microsoft.Data.SqlClient;
using System;

class Program
{
    const string connectionString = "Server=localhost;Database=IMDB3;Integrated Security=True;TrustServerCertificate=True;";

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
        Console.Write("Movie title: ");
        string title = Console.ReadLine();

        Console.Write("Original title (optional): ");
        string original = Console.ReadLine();
        if (String.IsNullOrWhiteSpace(original)) original = null;

        using var conn = new SqlConnection(connectionString);
        conn.Open();

        using var cmd = new SqlCommand("AddMovieSimple", conn);
        cmd.CommandType = System.Data.CommandType.StoredProcedure;
        cmd.Parameters.AddWithValue("@PrimaryTitle", title);
        cmd.Parameters.AddWithValue("@OriginalTitle", (object?)original ?? DBNull.Value);

        cmd.ExecuteNonQuery();

        Console.WriteLine("✅ Movie added");
    }

    static void AddPerson()
    {
        Console.Write("Person name: ");
        string name = Console.ReadLine();

        using var conn = new SqlConnection(connectionString);
        conn.Open();

        using var cmd = new SqlCommand("AddPerson", conn);
        cmd.CommandType = System.Data.CommandType.StoredProcedure;
        cmd.Parameters.AddWithValue("@Name", name);

        cmd.ExecuteNonQuery();

        Console.WriteLine("✅ Person added");
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
