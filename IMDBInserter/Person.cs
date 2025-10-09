using System;
using System.Text;

namespace IMDBInserter
{
    public class Person
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int BirthYear { get; set; }      
        public int? DeathYear { get; set; }     // Stadig nullable

        public string ToSql()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO Persons (Id, Name, BirthYear, DeathYear) VALUES (");

           
            sb.Append($"{Id}, ");

            
            sb.Append($"'{Name.Replace("'", "''")}', ");

            
            sb.Append($"{BirthYear}, ");

           
            sb.Append(DeathYear.HasValue ? DeathYear.Value.ToString() : "NULL");

            
            sb.Append(");");

            return sb.ToString();
        }
    }
}