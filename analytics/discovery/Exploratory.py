from abc import ABC, abstractmethod


class Exploratory(ABC):

    @abstractmethod
    def looking_missings(self):
        ...

    @abstractmethod
    def looking_missing_by(self):
        ...
