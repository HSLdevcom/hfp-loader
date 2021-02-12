import Knex, { Transaction } from 'knex'
import { PG_CONNECTION } from './constants'

let knexInstance: Knex | null = null

export function getKnex(): Knex {
  if (knexInstance instanceof Knex) {
    return knexInstance
  }

  knexInstance = Knex({
    dialect: 'postgres',
    client: 'pg',
    connection: PG_CONNECTION,
    pool: {
      min: 0,
      max: 300,
    },
  })

  return knexInstance
}

type Task = (trx: Transaction) => Promise<unknown>

class TransactionPool {
  isActive: boolean
  transactions: Transaction[] = []
  occupiedTransactions: number[] = []

  constructor(transactions: Transaction[]) {
    this.transactions = transactions
    this.isActive = true
  }

  static async createTransactions(count) {
    let knex = getKnex()
    let transactions: Transaction[] = []

    for (let i = 0; i < count; i++) {
      let trx = await knex.transaction()
      transactions.push(trx)
    }

    return transactions
  }

  async runWithTransaction(task: Task): Promise<unknown> {
    if (!this.isActive) {
      return Promise.reject('Transaction pool is inactive.')
    }

    return new Promise(async (resolve, reject) => {
      let trx: Transaction | undefined = undefined
      let iter = 0

      while (!trx && this.isActive && iter < 1000) {
        trx = await this.checkoutTransaction()

        if (!trx) {
          await new Promise((resolve) => setTimeout(resolve, 10))
          iter++
        }
      }

      if (!trx) {
        reject(
          this.isActive
            ? 'No transaction was found for the task within the timeframe.'
            : 'Pool is closed.'
        )
      } else {
        try {
          // console.log(`[TRX Pool] Transaction acquired, performing task.`)
          let taskResult = await task(trx)
          this.releaseTransaction(trx)
          resolve(taskResult)
        } catch (err) {
          console.error('[TRX Pool] Task error!', err)
          reject(this.rollbackTransaction(trx, err))
        }
      }
    })
  }

  async checkoutTransaction(): Promise<Transaction | undefined> {
    if (!this.isActive) {
      return
    }

    let trxLength = this.transactions.length
    let freeTrxIdx = -1

    for (let i = 0; i < trxLength; i++) {
      if (!this.occupiedTransactions.includes(i)) {
        freeTrxIdx = i
        break
      }
    }

    let trx = this.transactions[freeTrxIdx]

    if (!trx) {
      return
    }

    // Sanity check to not return a completed transaction. Should not really happen.
    // If it does happen, instead replace the closed one with a new transaction.
    if (trx.isCompleted()) {
      trx = await getKnex().transaction()
      this.transactions.splice(freeTrxIdx, 1, trx)
    }

    // console.log(`[TRX Pool] Transaction acquired ${freeTrxIdx}`)

    this.occupiedTransactions.push(freeTrxIdx)
    return trx
  }

  releaseTransaction(trx) {
    let trxIdx = this.transactions.indexOf(trx)

    if (trxIdx !== -1) {
      // console.log(`[TRX Pool] Transaction released ${trxIdx}`)
      let occupiedIdx = this.occupiedTransactions.indexOf(trxIdx)

      if (occupiedIdx !== -1) {
        this.occupiedTransactions.splice(occupiedIdx, 1)
      }
    }
  }

  async closePool() {
    this.isActive = false

    // console.log(`[TRX Pool] Transaction pool closed.`)

    return Promise.all(
      this.transactions.map((trx) => trx.executionPromise.then(() => trx.commit()))
    )
  }

  async rollbackTransaction(trx: Transaction, err?: any) {
    let trxIdx = this.transactions.indexOf(trx)

    if (trxIdx !== -1) {
      let newTrx = await getKnex().transaction()
      this.transactions.splice(trxIdx, 1, newTrx)
    }

    if (!trx.isCompleted()) {
      // console.log(`[TRX Pool] Transaction rolled back ${trxIdx}`)
      return trx.rollback(err)
    }

    return Promise.reject(err)
  }
}

export async function createTransactionPool(count: number) {
  let transactions = await TransactionPool.createTransactions(count)
  return new TransactionPool(transactions)
}
