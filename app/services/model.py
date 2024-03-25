class MyModel:
    def __init__(self, **kwargs):
        pass

    # For this template: Identity function =)
    def forward(self, x):
        return x

    @classmethod
    def from_pretrained(cls, model_folder):
        return cls()
