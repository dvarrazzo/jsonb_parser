from random import random, randint, randrange, choice
from typing import Any, List, Optional


class JsonFaker:
    def __init__(
        self,
        contchance: float = 0.66,
        contmax: int = 100,
        strmax: int = 100,
        keymax: int = 50,
    ):
        self.contchance = contchance
        self.contmax = contmax
        self.strmax = strmax
        self.keymax = keymax

    def random_json(self, contchance: Optional[float] = None) -> Any:
        if contchance is None:
            contchance = self.contchance
        if random() < contchance:
            return self.random_container(contchance=contchance)
        else:
            return self.random_scalar()

    def random_container(self, contchance: Optional[float] = None) -> Any:
        cont = choice([list, dict])
        if cont is list:
            return self.random_list(contchance=contchance)
        elif cont is dict:
            return self.random_object(contchance=contchance)
        else:
            assert False, f"unknown container type: {cont}"

    def random_scalar(self) -> Any:
        # TODO: add numbers to ubjson
        # typ = choice([bool, str, int, float])
        typ = choice([bool, str])
        meth = getattr(self, f"random_{typ.__name__}")
        return meth()

    def random_list(self, contchance: Optional[float] = None) -> Any:
        if contchance is None:
            contchance = self.contchance
        return [
            self.random_json(contchance=contchance / 2.0)
            for i in range(randrange(self.contmax))
        ]

    def random_object(self, contchance: Optional[float] = None) -> Any:
        if contchance is None:
            contchance = self.contchance
        return {
            self.random_str(self.keymax): self.random_json(
                contchance=contchance / 2.0
            )
            for i in range(randrange(self.contmax))
        }

    def random_str(
        self, strmax: Optional[int] = None, unichance: float = 0.2
    ) -> str:
        if strmax is None:
            strmax = self.strmax

        length = randrange(strmax)

        rv: List[int] = []
        while len(rv) < length:
            if random() < unichance:
                c = randrange(1, 0x110000)
                if 0xD800 <= c <= 0xDBFF or 0xDC00 <= c <= 0xDFFF:
                    continue
            else:
                c = randrange(1, 128)
            rv.append(c)

        return "".join(map(chr, rv))

    def random_bool(self) -> Optional[bool]:
        # I give you a None for free too
        return choice([None, True, False])

    def random_int(self) -> int:
        return randint(-100000000000000000000, 1000000000000000000)

    def random_float(self) -> float:
        n = self.random_int()
        return n * 10 ^ randint(-20, 20)
