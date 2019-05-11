const Sequelize = require('sequelize');
const config = {
    pool: {
        max: 5,
        min: 0,
        acquire: 30000,
        idle: 10000
    }
}
const sequelize = new Sequelize('postgres://postgres:Welcome@pwc01@localhost:5432/test',config);

sequelize
  .authenticate()
  .then(() => {
    console.log('Connection has been established successfully.');
  })
  .catch(err => {
    console.error('Unable to connect to the database:', err);
  });