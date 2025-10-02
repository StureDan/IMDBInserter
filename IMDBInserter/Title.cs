using System;
using System.Collections.Generic;
using System.Text;

namespace IMDBInserter
{
    public class Title
    {
        public int Id { get; set; }
        public int TitleTypeId { get; set; }
        public string PrimaryTitle { get; set; }
        public string OriginalTitle { get; set; }
        public bool IsAdult { get; set; }
        public int? StartYear { get; set; }       // nullable
        public int? EndYear { get; set; }         // nullable
        public int? RuntimeMinutes { get; set; }  // nullable


        public string ToSql()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO Titles (Id, TitleType, PrimaryTitle, OriginalTitle, " +
                      "IsAdult, StartYear, EndYear, RuntimeMinutes) VALUES (");

            sb.Append($"{Id}, ");
            sb.Append($"{TitleTypeId}, ");
            sb.Append($"'{PrimaryTitle?.Replace("'", "''")}', ");
            sb.Append(OriginalTitle == null ? "NULL, " : $"'{OriginalTitle.Replace("'", "''")}', ");
            sb.Append($"{(IsAdult ? 1 : 0)}, ");
            sb.Append(StartYear.HasValue ? $"{StartYear.Value}, " : "NULL, ");
            sb.Append(EndYear.HasValue ? $"{EndYear.Value}, " : "NULL, ");
            sb.Append(RuntimeMinutes.HasValue ? $"{RuntimeMinutes.Value}, " : "NULL, ");

            return sb.ToString();
        }
    }
}
