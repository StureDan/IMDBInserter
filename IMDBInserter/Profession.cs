using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBInserter
{
    public class Profession
    {
        public int Id { get; set; }
        public string Name { get; set; }
        

        public string ToSql()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO Profession (Id, Name) VALUES (");


            sb.Append($"{Id}, ");
            sb.Append($"'{Name.Replace("'", "''")}', ");

            sb.Append(");");

            return sb.ToString();
        }
    }
}
