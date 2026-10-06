using Microsoft.EntityFrameworkCore;
using RabbitConsumer.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;

namespace RabbitConsumer.Data;


public class MyDbContext : DbContext
{
    public MyDbContext(DbContextOptions<MyDbContext> options)
    : base(options)
    {
    }
    public DbSet<OverseasAlert> Overseas { get; set; }
    public DbSet<SouthAlert> South { get; set; }
    public DbSet<NorthAlert> North { get; set; }
    public DbSet<CenterAlert> Center { get; set; }
}

