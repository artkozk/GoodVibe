from __future__ import annotations

import hashlib
import json
import math
import os
import random
from dataclasses import dataclass


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


@dataclass
class TinyFeedbackModel:
    input_dim: int
    hidden_dim: int
    w1: list[list[float]]
    b1: list[float]
    w2: list[float]
    b2: float
    train_updates: int

    @classmethod
    def create(cls, input_dim: int = 128, hidden_dim: int = 24, seed: int = 42) -> TinyFeedbackModel:
        rnd = random.Random(seed)
        w1 = [[rnd.uniform(-0.08, 0.08) for _ in range(input_dim)] for _ in range(hidden_dim)]
        b1 = [0.0 for _ in range(hidden_dim)]
        w2 = [rnd.uniform(-0.08, 0.08) for _ in range(hidden_dim)]
        b2 = 0.0
        return cls(
            input_dim=input_dim,
            hidden_dim=hidden_dim,
            w1=w1,
            b1=b1,
            w2=w2,
            b2=b2,
            train_updates=0,
        )

    @classmethod
    def load_or_create(cls, path: str) -> TinyFeedbackModel:
        if not os.path.exists(path):
            return cls.create()

        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)

        return cls(
            input_dim=int(raw["input_dim"]),
            hidden_dim=int(raw["hidden_dim"]),
            w1=[[float(x) for x in row] for row in raw["w1"]],
            b1=[float(x) for x in raw["b1"]],
            w2=[float(x) for x in raw["w2"]],
            b2=float(raw["b2"]),
            train_updates=int(raw.get("train_updates", 0)),
        )

    def save(self, path: str) -> None:
        payload = {
            "input_dim": self.input_dim,
            "hidden_dim": self.hidden_dim,
            "w1": self.w1,
            "b1": self.b1,
            "w2": self.w2,
            "b2": self.b2,
            "train_updates": self.train_updates,
        }
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
        os.replace(tmp_path, path)

    def _hash_feature(self, key: str) -> tuple[int, float]:
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        num = int.from_bytes(digest[:8], "big")
        idx = num % self.input_dim
        sign = 1.0 if (num >> 63) == 0 else -1.0
        return idx, sign

    def _vectorize(self, features: dict[str, float]) -> list[float]:
        vector = [0.0 for _ in range(self.input_dim)]
        for key, value in features.items():
            idx, sign = self._hash_feature(key)
            vector[idx] += sign * float(value)

        norm = math.sqrt(sum(v * v for v in vector))
        if norm > 0:
            vector = [v / norm for v in vector]
        return vector

    def predict(self, features: dict[str, float]) -> float:
        x = self._vectorize(features)
        hidden = []
        for j in range(self.hidden_dim):
            z1 = self.b1[j] + sum(self.w1[j][i] * x[i] for i in range(self.input_dim))
            hidden.append(math.tanh(z1))
        z2 = self.b2 + sum(self.w2[j] * hidden[j] for j in range(self.hidden_dim))
        return _sigmoid(z2)

    def train(self, features: dict[str, float], target: float, lr: float = 0.05, epochs: int = 10) -> None:
        y_true = 1.0 if target >= 0.5 else 0.0
        x = self._vectorize(features)

        for _ in range(max(1, epochs)):
            z1 = [0.0 for _ in range(self.hidden_dim)]
            h = [0.0 for _ in range(self.hidden_dim)]

            for j in range(self.hidden_dim):
                value = self.b1[j] + sum(self.w1[j][i] * x[i] for i in range(self.input_dim))
                z1[j] = value
                h[j] = math.tanh(value)

            z2 = self.b2 + sum(self.w2[j] * h[j] for j in range(self.hidden_dim))
            y_pred = _sigmoid(z2)
            dz2 = y_pred - y_true

            old_w2 = list(self.w2)
            for j in range(self.hidden_dim):
                grad_w2 = dz2 * h[j]
                self.w2[j] -= lr * grad_w2
            self.b2 -= lr * dz2

            for j in range(self.hidden_dim):
                dz1_j = dz2 * old_w2[j] * (1.0 - h[j] * h[j])
                for i in range(self.input_dim):
                    grad_w1 = dz1_j * x[i]
                    self.w1[j][i] -= lr * grad_w1
                self.b1[j] -= lr * dz1_j

        self.train_updates += 1

    def model_info(self) -> str:
        return (
            f"MLP(input={self.input_dim}, hidden={self.hidden_dim}, "
            f"train_updates={self.train_updates})"
        )
