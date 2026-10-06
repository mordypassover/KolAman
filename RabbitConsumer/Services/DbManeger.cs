using Amqp;
using Microsoft.EntityFrameworkCore;
using RabbitConsumer.Data;
using RabbitConsumer.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitConsumer.Services
{
    public class DbManeger
    {
        private readonly MyDbContext _context;

        public DbManeger(MyDbContext context)
        {
            _context = context;
        }
        public async Task<bool> Store(IncommingMesege message, string db)
        {
            Console.WriteLine("storring");
            if (db == "OVERSEAS")
            {
                OverseasAlert newAlert = new OverseasAlert
                {
                    AlertId = message.AlertId,
                    Source = message.Source,
                    Title = message.Title,
                    Content = message.Content,
                    Priority = message.Priority,
                    Classification = message.Classification,
                    Lat = message.Lat,
                    Lon = message.Lon,
                    Timestamp = message.Timestamp,
                    Status = message.Status

                };
                var exists = await _context.Overseas.FindAsync(message.AlertId);
                if (exists != null)
                {
                    _context.Overseas.Remove(exists);
                }
                _context.Overseas.Add(newAlert);
                _context.SaveChanges();
            }
            else if (db == "SOUTH")
            {
                SouthAlert newAlert = new SouthAlert
                {
                    AlertId = message.AlertId,
                    Source = message.Source,
                    Title = message.Title,
                    Content = message.Content,
                    Priority = message.Priority,
                    Classification = message.Classification,
                    Lat = message.Lat,
                    Lon = message.Lon,
                    Timestamp = message.Timestamp,
                    Status = message.Status

                };
                var exists = await _context.South.FindAsync(message.AlertId);
                if (exists != null)
                {
                    _context.South.Remove(exists);
                }
                _context.South.Add(newAlert);
                _context.SaveChanges();

            }
            else if(db == "NORTH")
            {
                NorthAlert newAlert = new NorthAlert
                {
                    AlertId = message.AlertId,
                    Source = message.Source,
                    Title = message.Title,
                    Content = message.Content,
                    Priority = message.Priority,
                    Classification = message.Classification,
                    Lat = message.Lat,
                    Lon = message.Lon,
                    Timestamp = message.Timestamp,
                    Status = message.Status

                };
                var exists = await _context.North.FindAsync(message.AlertId);
                if (exists != null)
                {
                    _context.North.Remove(exists);
                }
                _context.North.Add(newAlert);
                _context.SaveChanges();

            }
            else if(db == "CENTER")
            {
                CenterAlert newAlert = new CenterAlert
                {
                    AlertId = message.AlertId,
                    Source = message.Source,
                    Title = message.Title,
                    Content = message.Content,
                    Priority = message.Priority,
                    Classification = message.Classification,
                    Lat = message.Lat,
                    Lon = message.Lon,
                    Timestamp = message.Timestamp,
                    Status = message.Status

                };
                var exists = await _context.Center.FindAsync(message.AlertId);
                if (exists != null)
                {
                    _context.Center.Remove(exists);
                }
                _context.Center.Add(newAlert);
                _context.SaveChanges();
                
            }
            else
            {
                return false;
            }
            return true;







        }
    }
}
