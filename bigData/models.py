from django.db import models

# Create your models here.

class DataRecord(models.Model):
    nome = models.CharField(max_length=100)
    cnpj = models.CharField(max_length=20)
    timestamp = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.nome}: {self.cnpj} at {self.timestamp}"