using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace RabbitConsumer.Models;

public class OverseasAlert
{
    [Required]
    [Key]
    [JsonPropertyName("alert_id")]
    public string AlertId { get; set; }

    [Required]
    [JsonPropertyName("source")]
    public string Source { get; set; }
    [Required]
    [JsonPropertyName("title")]
    public string Title { get; set; }
    [Required]
    [JsonPropertyName("content")]
    public string Content { get; set; }
    [Required]
    [JsonPropertyName("priority")]
    public string Priority { get; set; }
    [Required]
    [JsonPropertyName("classification")]
    public string Classification { get; set; }
    [Required]
    [JsonPropertyName("lat")]
    public double Lat { get; set; }
    [Required]
    [JsonPropertyName("lon")]
    public double Lon { get; set; }
    [Required]
    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }
    [Required]
    [JsonPropertyName("status")]
    public string Status { get; set; }

}
