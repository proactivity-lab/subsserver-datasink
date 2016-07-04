from unittest import TestCase

from subsdatasink.subsdatasink import process_data, DataElement


__author__ = "Raido Pahtma"
__license__ = "MIT"


class TestDataProcessing(TestCase):

    def test_single_value(self):
        data = {"source": "0011223344556677",
                "type": "dt_some_data",
                "value": 0.001,
                "timestamp_production": 1425661616.000,
                "duration_production": 5.0,
                "timestamp_arrival": 1425661680.100}

        root = process_data(None, [data])
        self.assertEqual(len(root.data), 0)

        e = root
        self.assertIsInstance(e, DataElement)

        self.assertEqual(data["source"], e.source)
        self.assertEqual(data["type"], e.type)
        self.assertEqual(data["value"], e.value)
        self.assertEqual(data["timestamp_production"], e.production_start)
        self.assertEqual(data["timestamp_production"] + data["duration_production"], e.production_end)
        self.assertEqual(data["timestamp_arrival"], e.arrival)

    def test_multiple_value(self):
        data = {"source": "0011223344556677",
                "type": "dt_some_data",
                "values": [
                    {"value": 0.001, "timestamp_production": 1425661616.000, "duration_production": 1.0},
                    {"value": 0.002, "timestamp_production": 1425661617.000, "duration_production": 1.0},
                    {"value": 0.003, "timestamp_production": 1425661618.000, "duration_production": 1.0},
                ],
                "timestamp_arrival": 1425661680.100}

        root = process_data(None, [data])
        self.assertEqual(len(root.data), len(data["values"]))

        for idx in range(0, len(data["values"])):
            e = root.data[idx]
            self.assertIsInstance(e, DataElement)

            self.assertEqual(data["source"], e.source)
            self.assertEqual(data["type"], e.type)
            self.assertEqual(data["values"][idx]["value"], e.value)
            self.assertEqual(data["values"][idx]["timestamp_production"], e.production_start)
            self.assertEqual(data["values"][idx]["timestamp_production"] + data["values"][idx]["duration_production"], e.production_end)
            self.assertEqual(data["timestamp_arrival"], e.arrival)

    def test_no_timestamp(self):
        data = {"source": "0011223344556677",
                "type": "dt_some_data",
                "values": [
                    {"value": 0.001},
                    {"value": 0.002, "duration_production": 1.0},
                    {"value": 0.003, "timestamp_production": 1425661618.000},
                    {"value": 0.004, "timestamp_production": 1425661618.000, "duration_production": 1.0}
                ],
                "timestamp_arrival": 1425661680.100}

        root = process_data(None, [data])
        self.assertEqual(len(root.data), len(data["values"]))

        for idx in range(0, len(data["values"])):
            print idx
            e = root.data[idx]
            self.assertIsInstance(e, DataElement)

            self.assertEqual(data["source"], e.source)
            self.assertEqual(data["type"], e.type)
            self.assertEqual(data["values"][idx]["value"], e.value)
            if "timestamp_production" in data["values"][idx]:
                self.assertEqual(data["values"][idx]["timestamp_production"], e.production_start)
                if "duration_production" in data["values"][idx]:
                    self.assertEqual(data["values"][idx]["timestamp_production"] + data["values"][idx]["duration_production"], e.production_end)
                else:
                    self.assertEqual(data["values"][idx]["timestamp_production"], e.production_end)
            else:
                self.assertEqual(None, e.production_start)
                self.assertEqual(None, e.production_end)
            self.assertEqual(data["timestamp_arrival"], e.arrival)
