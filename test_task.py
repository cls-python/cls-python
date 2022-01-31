import random

import luigi


class Block(luigi.Task):
    filename = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(self.filename, is_tmp=False)

    def run(self):
        user_input = input("Say something: ")
        print(f"writing {user_input} to {self.filename}")
        randrange_tmp = random.randrange
        def randrange(start, stop):
            return randrange_tmp(int(start), int(stop))
        random.randrange = randrange
        with self.output().open('w') as outfile:
            outfile.write(user_input)
        random.randrange = randrange_tmp


class BlockSpawn(luigi.Task):
    position = luigi.IntParameter(default=0)
    accepts_messages = True

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget('dummy.txt', is_tmp=True)

    def run(self):
        print(f"BlockSpawn (re)started {self.position}!")
        self.position += 1
        while True:
            if not self.scheduler_messages.empty():
                msg = self.scheduler_messages.get()
                if msg.content == "terminate":
                    break
                print(f"Starting Block(filename={msg.content})")
                yield Block(filename=msg.content)
        print("Stopping")


if __name__ == '__main__':
    luigi.build([BlockSpawn()])



